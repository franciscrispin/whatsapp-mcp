package main

// Applying and removing a label on one chat.
//
// This is the only label write the bridge exposes. Creating, renaming and
// deleting labels are deliberately not here: an unknown label id is rejected
// rather than letting WhatsApp create an association to a label that does not
// exist. Writes are paced by a small rate limit so a runaway caller stalls
// instead of relabelling a whole account.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const (
	labelWriteLimit  = 10
	labelWriteWindow = time.Minute
)

// LabelApplyRequest is the request body for /api/labels/apply.
type LabelApplyRequest struct {
	ChatJID string `json:"chat_jid"` // phone number, phone JID, LID or group JID
	LabelID string `json:"label_id"` // an existing, non-deleted label
	Labeled *bool  `json:"labeled"`  // true applies the label, false removes it
}

// rateLimiter allows at most `limit` calls in any sliding `window`.
type rateLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	calls  []time.Time
}

func (l *rateLimiter) allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	cutoff := now.Add(-l.window)
	kept := l.calls[:0]
	for _, t := range l.calls {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	l.calls = kept
	if len(l.calls) >= l.limit {
		return false
	}
	l.calls = append(l.calls, now)
	return true
}

var labelWriteLimiter = &rateLimiter{limit: labelWriteLimit, window: labelWriteWindow}

// lidLookup is the part of whatsmeow's LID store the bridge needs here.
type lidLookup interface {
	GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error)
}

var errNoLIDForPhone = errors.New("no LID known for this phone number")

// resolveLabelTarget turns caller input into the JID WhatsApp keys label
// associations by. One-to-one chats are labelled by LID, so a phone number is
// mapped through the local LID store only: no lookup is sent to WhatsApp.
func resolveLabelTarget(ctx context.Context, lids lidLookup, raw string) (types.JID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return types.JID{}, errors.New("chat_jid is required")
	}
	var jid types.JID
	if strings.Contains(raw, "@") {
		parsed, err := types.ParseJID(raw)
		if err != nil {
			return types.JID{}, fmt.Errorf("invalid chat_jid: %w", err)
		}
		jid = parsed.ToNonAD()
	} else {
		jid = types.NewJID(strings.TrimPrefix(raw, "+"), types.DefaultUserServer)
	}

	switch jid.Server {
	case types.HiddenUserServer, types.GroupServer:
		return jid, nil
	case types.DefaultUserServer:
		lid, err := lids.GetLIDForPN(ctx, jid)
		if err != nil {
			return types.JID{}, fmt.Errorf("LID lookup failed for %s: %w", jid, err)
		}
		if lid.IsEmpty() {
			return types.JID{}, fmt.Errorf("%w: %s", errNoLIDForPhone, jid.User)
		}
		return lid, nil
	default:
		return types.JID{}, fmt.Errorf("cannot label a %s chat", jid.Server)
	}
}

// liveLabelExists reports whether label_id is a known label that has not been
// deleted.
func (store *MessageStore) liveLabelExists(labelID string) (bool, error) {
	var n int
	err := store.db.QueryRow(`SELECT COUNT(*) FROM labels WHERE label_id = ? AND deleted = 0`, labelID).Scan(&n)
	return n > 0, err
}

func (store *MessageStore) chatHasLabel(labelID, chatJID string) (bool, error) {
	var n int
	err := store.db.QueryRow(
		`SELECT COUNT(*) FROM label_chats WHERE label_id = ? AND chat_jid = ? AND labeled = 1`,
		labelID, chatJID).Scan(&n)
	return n > 0, err
}

func registerLabelApplyEndpoint(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc, client *whatsmeow.Client, messageStore *MessageStore) {
	mux.HandleFunc("/api/labels/apply", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fail := func(status int, msg string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": msg})
		}

		var req LabelApplyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.LabelID == "" || req.Labeled == nil {
			fail(http.StatusBadRequest, "chat_jid, label_id and labeled are required")
			return
		}
		fmt.Printf("→ /api/labels/apply chat=%q label=%q labeled=%t from=%q\n", req.ChatJID, req.LabelID, *req.Labeled, r.RemoteAddr)

		if !client.IsConnected() {
			fail(http.StatusServiceUnavailable, "Not connected to WhatsApp")
			return
		}
		live, err := messageStore.liveLabelExists(req.LabelID)
		if err != nil {
			fail(http.StatusInternalServerError, fmt.Sprintf("label lookup failed: %v", err))
			return
		}
		if !live {
			fail(http.StatusNotFound, fmt.Sprintf("no such label: %s", req.LabelID))
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		target, err := resolveLabelTarget(ctx, client.Store.LIDs, req.ChatJID)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errNoLIDForPhone) {
				status = http.StatusNotFound
			}
			fail(status, err.Error())
			return
		}

		wasLabeled, err := messageStore.chatHasLabel(req.LabelID, target.String())
		if err != nil {
			fail(http.StatusInternalServerError, fmt.Sprintf("label lookup failed: %v", err))
			return
		}
		if !labelWriteLimiter.allow(time.Now()) {
			fail(http.StatusTooManyRequests, fmt.Sprintf("label write limit reached (%d per minute)", labelWriteLimit))
			return
		}

		// Not client.SendAppState: it leaves the copy under an older key behind
		// and splits the state hash (see app_state_write.go).
		if err := sendAppStateOverwrite(ctx, client, messageStore.waDB, appstate.BuildLabelChat(target, req.LabelID, *req.Labeled)); err != nil {
			fail(http.StatusBadGateway, fmt.Sprintf("WhatsApp rejected the label change: %v", err))
			return
		}
		// The write's sync-back also dispatches the change back as an event, which
		// stores it; writing it here as well makes the result readable at once.
		if err := messageStore.StoreLabelChat(&events.LabelAssociationChat{
			JID: target, LabelID: req.LabelID, Timestamp: time.Now(),
			Action: &waSyncAction.LabelAssociationAction{Labeled: proto.Bool(*req.Labeled)},
		}); err != nil {
			fmt.Printf("Label applied but not stored locally: %v\n", err)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"result": map[string]interface{}{
				"chat_jid":    target.String(),
				"label_id":    req.LabelID,
				"labeled":     *req.Labeled,
				"was_labeled": wasLabeled,
			},
		})
	}))
}
