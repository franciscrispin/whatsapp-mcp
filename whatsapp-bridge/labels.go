package main

// Label store.
//
// WhatsApp Business labels ("Lists" in the app) live only in the "regular" app
// state collection. whatsmeow does not persist them: it emits LabelEdit and
// LabelAssociationChat events as changes arrive and keeps nothing decoded. This
// file stores those events in messages.db so labels can be read with plain SQL
// (the MCP server reads them directly; the bridge does not need to be up).
//
// Two whatsmeow behaviours shape the design:
//
//   - A full sync only emits events when EmitAppStateEventsOnFullSync is set,
//     so the resync below switches it on for the duration of the call.
//   - Mutations with a REMOVE operation are dropped before dispatch. A label
//     removed that way never produces an event, so the store can go stale. A
//     full resync replays the current state, and anything it does not mention
//     is marked removed (labels.deleted = 1, label_chats.labeled = 0).
//
// Removed rows are kept rather than deleted so the tables double as a history.
// Message-level labels (LabelAssociationMessage) are not stored.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

const labelSchema = `
	CREATE TABLE IF NOT EXISTS labels (
		label_id   TEXT PRIMARY KEY,
		name       TEXT NOT NULL,
		color      INTEGER NOT NULL,
		deleted    INTEGER NOT NULL DEFAULT 0,
		updated_at TIMESTAMP NOT NULL,
		synced_at  INTEGER NOT NULL -- unix nanoseconds, when the bridge last wrote the row
	);

	CREATE TABLE IF NOT EXISTS label_chats (
		label_id   TEXT NOT NULL,
		chat_jid   TEXT NOT NULL,
		labeled    INTEGER NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		synced_at  INTEGER NOT NULL,
		PRIMARY KEY (label_id, chat_jid)
	);

	CREATE INDEX IF NOT EXISTS idx_label_chats_chat ON label_chats(chat_jid);
`

// labelResyncMu serialises full resyncs so two cannot interleave their
// reconciliation passes.
var labelResyncMu sync.Mutex

func ensureLabelSchema(db *sql.DB) error {
	if _, err := db.Exec(labelSchema); err != nil {
		return fmt.Errorf("failed to create label tables: %w", err)
	}
	return nil
}

// StoreLabelEdit records a label being created, renamed, recoloured or deleted.
func (store *MessageStore) StoreLabelEdit(evt *events.LabelEdit) error {
	_, err := store.db.Exec(`
		INSERT INTO labels (label_id, name, color, deleted, updated_at, synced_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(label_id) DO UPDATE SET
			name = excluded.name, color = excluded.color, deleted = excluded.deleted,
			updated_at = excluded.updated_at, synced_at = excluded.synced_at`,
		evt.LabelID, evt.Action.GetName(), evt.Action.GetColor(), evt.Action.GetDeleted(),
		evt.Timestamp, time.Now().UnixNano())
	return err
}

// StoreLabelChat records a label being applied to or removed from a chat. The
// chat JID is kept in the form WhatsApp sent it (usually @lid for one-to-one
// chats); readers resolve it to a phone JID via whatsmeow_lid_map.
func (store *MessageStore) StoreLabelChat(evt *events.LabelAssociationChat) error {
	_, err := store.db.Exec(`
		INSERT INTO label_chats (label_id, chat_jid, labeled, updated_at, synced_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(label_id, chat_jid) DO UPDATE SET
			labeled = excluded.labeled, updated_at = excluded.updated_at,
			synced_at = excluded.synced_at`,
		evt.LabelID, evt.JID.String(), evt.Action.GetLabeled(), evt.Timestamp, time.Now().UnixNano())
	return err
}

// markUnsyncedLabelsRemoved marks every label and association not written
// since `since` as removed. Only call it after a successful full resync.
func (store *MessageStore) markUnsyncedLabelsRemoved(since time.Time) (labels, chats int64, err error) {
	res, err := store.db.Exec(`UPDATE labels SET deleted = 1 WHERE synced_at < ? AND deleted = 0`, since.UnixNano())
	if err != nil {
		return 0, 0, err
	}
	labels, _ = res.RowsAffected()
	res, err = store.db.Exec(`UPDATE label_chats SET labeled = 0 WHERE synced_at < ? AND labeled = 1`, since.UnixNano())
	if err != nil {
		return labels, 0, err
	}
	chats, _ = res.RowsAffected()
	return labels, chats, nil
}

func (store *MessageStore) countLabels() (labels, chats int, err error) {
	err = store.db.QueryRow(`
		SELECT (SELECT COUNT(*) FROM labels WHERE deleted = 0),
		       (SELECT COUNT(*) FROM label_chats WHERE labeled = 1)`).Scan(&labels, &chats)
	return labels, chats, err
}

func handleLabelEvent(messageStore *MessageStore, evt interface{}) {
	var err error
	switch v := evt.(type) {
	case *events.LabelEdit:
		err = messageStore.StoreLabelEdit(v)
	case *events.LabelAssociationChat:
		err = messageStore.StoreLabelChat(v)
	default:
		return
	}
	if err != nil {
		fmt.Printf("Failed to store label event: %v\n", err)
	}
}

type labelResyncResult struct {
	Labels        int   `json:"labels"`
	Associations  int   `json:"associations"`
	LabelsRemoved int64 `json:"labels_marked_removed"`
	ChatsRemoved  int64 `json:"associations_marked_removed"`
}

// resyncLabels replays the full label state from WhatsApp into the store and
// reconciles anything the replay no longer contains. Read-only towards
// WhatsApp: it fetches app state and sends nothing.
func resyncLabels(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore) (labelResyncResult, error) {
	labelResyncMu.Lock()
	defer labelResyncMu.Unlock()

	start := time.Now()
	client.EmitAppStateEventsOnFullSync = true
	err := client.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
	client.EmitAppStateEventsOnFullSync = false
	if err != nil {
		return labelResyncResult{}, fmt.Errorf("full app state sync failed: %w", err)
	}

	var result labelResyncResult
	result.LabelsRemoved, result.ChatsRemoved, err = messageStore.markUnsyncedLabelsRemoved(start)
	if err != nil {
		return result, fmt.Errorf("failed to reconcile labels: %w", err)
	}
	result.Labels, result.Associations, err = messageStore.countLabels()
	return result, err
}

// resyncLabelsIfEmpty fills the store on first run so labels are readable
// without anyone having to call the endpoint.
func resyncLabelsIfEmpty(client *whatsmeow.Client, messageStore *MessageStore) {
	labels, _, err := messageStore.countLabels()
	if err != nil || labels > 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := resyncLabels(ctx, client, messageStore)
	if err != nil {
		fmt.Printf("Initial label sync failed: %v\n", err)
		return
	}
	fmt.Printf("Initial label sync: %d labels, %d chat associations\n", result.Labels, result.Associations)
}

func registerLabelEndpoints(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc, client *whatsmeow.Client, messageStore *MessageStore) {
	mux.HandleFunc("/api/labels/resync", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Printf("→ /api/labels/resync from=%q user_agent=%q\n", r.RemoteAddr, r.UserAgent())

		w.Header().Set("Content-Type", "application/json")
		if !client.IsConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false, "message": "Not connected to WhatsApp",
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 50*time.Second)
		defer cancel()
		result, err := resyncLabels(ctx, client, messageStore)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": result})
	}))

	registerLabelApplyEndpoint(mux, auth, client, messageStore)
}
