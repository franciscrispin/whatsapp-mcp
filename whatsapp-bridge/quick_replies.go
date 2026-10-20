package main

// Quick reply store.
//
// WhatsApp Business quick replies ("/shortcut" canned messages) live in the
// "regular" app state collection, next to labels, keyed
// ["quick_reply", <id>]. whatsmeow has no typed event for them: they arrive
// only as the generic events.AppState, which this file stores in messages.db so
// they can be read with plain SQL.
//
// The label store's caveats apply unchanged (see labels.go): a full sync only
// emits events while EmitAppStateEventsOnFullSync is set, and a REMOVE
// mutation is dropped before dispatch, so a quick reply deleted that way only
// shows up as deleted after a full resync. Deleted rows are kept, not removed.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

const quickReplySchema = `
	CREATE TABLE IF NOT EXISTS quick_replies (
		quick_reply_id TEXT PRIMARY KEY,
		shortcut       TEXT NOT NULL,
		message        TEXT NOT NULL,
		keywords       TEXT NOT NULL, -- JSON array of strings
		label_ids      TEXT NOT NULL, -- JSON array of label ids the reply is linked to
		use_count      INTEGER NOT NULL,
		deleted        INTEGER NOT NULL DEFAULT 0,
		updated_at     TIMESTAMP NOT NULL,
		synced_at      INTEGER NOT NULL -- unix nanoseconds, when the bridge last wrote the row
	);
`

func ensureQuickReplySchema(db *sql.DB) error {
	if _, err := db.Exec(quickReplySchema); err != nil {
		return fmt.Errorf("failed to create quick reply table: %w", err)
	}
	return nil
}

func jsonStrings(values []string) string {
	if values == nil {
		values = []string{}
	}
	b, _ := json.Marshal(values)
	return string(b)
}

// StoreQuickReply records a quick reply being created, edited or deleted.
// Events for any other app state index are ignored.
func (store *MessageStore) StoreQuickReply(evt *events.AppState) (bool, error) {
	if len(evt.Index) < 2 || evt.Index[0] != appstate.IndexQuickReply || evt.SyncActionValue == nil {
		return false, nil
	}
	act := evt.GetQuickReplyAction()
	if act == nil {
		return false, nil
	}
	updatedAt := time.Now()
	if ts := evt.GetTimestamp(); ts > 0 {
		updatedAt = time.UnixMilli(ts)
	}
	_, err := store.db.Exec(`
		INSERT INTO quick_replies
			(quick_reply_id, shortcut, message, keywords, label_ids, use_count, deleted, updated_at, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(quick_reply_id) DO UPDATE SET
			shortcut = excluded.shortcut, message = excluded.message, keywords = excluded.keywords,
			label_ids = excluded.label_ids, use_count = excluded.use_count, deleted = excluded.deleted,
			updated_at = excluded.updated_at, synced_at = excluded.synced_at`,
		evt.Index[1], act.GetShortcut(), act.GetMessage(), jsonStrings(act.GetKeywords()),
		jsonStrings(act.GetAssociatedLabelIDs()), act.GetCount(), act.GetDeleted(),
		updatedAt, time.Now().UnixNano())
	return true, err
}

// markUnsyncedQuickRepliesRemoved marks every quick reply not written since
// `since` as deleted. Only call it after a successful full resync.
func (store *MessageStore) markUnsyncedQuickRepliesRemoved(since time.Time) (int64, error) {
	res, err := store.db.Exec(`UPDATE quick_replies SET deleted = 1 WHERE synced_at < ? AND deleted = 0`, since.UnixNano())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (store *MessageStore) countQuickReplies() (int, error) {
	var n int
	err := store.db.QueryRow(`SELECT COUNT(*) FROM quick_replies WHERE deleted = 0`).Scan(&n)
	return n, err
}

func handleQuickReplyEvent(messageStore *MessageStore, evt *events.AppState) {
	if _, err := messageStore.StoreQuickReply(evt); err != nil {
		fmt.Printf("Failed to store quick reply: %v\n", err)
	}
}

type quickReplyResyncResult struct {
	QuickReplies int   `json:"quick_replies"`
	Removed      int64 `json:"marked_removed"`
}

// resyncQuickReplies replays the "regular" collection from WhatsApp into the
// store and marks anything the replay no longer contains as deleted. It takes
// labelResyncMu because the label resync toggles the same client flag over the
// same collection; interleaving the two would leave one replay without events
// and mark everything removed. Read-only towards WhatsApp.
func resyncQuickReplies(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore) (quickReplyResyncResult, error) {
	labelResyncMu.Lock()
	defer labelResyncMu.Unlock()

	start := time.Now()
	client.EmitAppStateEventsOnFullSync = true
	err := client.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
	client.EmitAppStateEventsOnFullSync = false
	if err != nil {
		return quickReplyResyncResult{}, fmt.Errorf("full app state sync failed: %w", err)
	}

	var result quickReplyResyncResult
	if result.Removed, err = messageStore.markUnsyncedQuickRepliesRemoved(start); err != nil {
		return result, fmt.Errorf("failed to reconcile quick replies: %w", err)
	}
	result.QuickReplies, err = messageStore.countQuickReplies()
	return result, err
}

// resyncQuickRepliesIfEmpty fills the store on first run so quick replies are
// readable without anyone having to call the endpoint.
func resyncQuickRepliesIfEmpty(client *whatsmeow.Client, messageStore *MessageStore) {
	n, err := messageStore.countQuickReplies()
	if err != nil || n > 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// A full sync drops the local state first, so never start one by itself
	// over a collection that already syncs: if the server snapshot failed to
	// verify, the working state would be lost on every restart.
	if v, _, err := client.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular)); err != nil || v > 0 {
		return
	}
	result, err := resyncQuickReplies(ctx, client, messageStore)
	if err != nil {
		fmt.Printf("Initial quick reply sync failed: %v\n", err)
		return
	}
	fmt.Printf("Initial quick reply sync: %d quick replies\n", result.QuickReplies)
}

func registerQuickReplyEndpoints(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc, client *whatsmeow.Client, messageStore *MessageStore) {
	mux.HandleFunc("/api/quick-replies/resync", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Printf("→ /api/quick-replies/resync from=%q user_agent=%q\n", r.RemoteAddr, r.UserAgent())

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
		result, err := resyncQuickReplies(ctx, client, messageStore)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": result})
	}))
}
