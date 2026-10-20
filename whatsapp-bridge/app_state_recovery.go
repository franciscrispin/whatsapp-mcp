package main

// App state recovery.
//
// When the bridge's copy of an app state collection stops verifying against
// the server ("mismatching LTHash"), every later patch for that collection is
// refused, so labels, quick replies or contact names stop syncing and writes
// to the collection fail with 409. A full resync from the server does not help
// when the server's own snapshot fails the same check.
//
// WhatsApp's fix is to ask the primary phone for a copy of the collection.
// whatsmeow builds the request and applies the phone's reply on its own
// (handleAppStateRecovery), replacing the stored version, hash and MACs. The
// phone only answers while WhatsApp is running on it, so the endpoint sends the
// request and returns; the reply is handled whenever it arrives. The request
// goes only to the account's own phone; no contact sees anything.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

// appStateRecoveryWindow is how long a request stays open. The phone answers
// only while WhatsApp is running on it, so the reply can take a while.
const appStateRecoveryWindow = 30 * time.Minute

type pendingRecovery struct {
	sentAt time.Time
	timer  *time.Timer
}

var (
	recoveryMu      sync.Mutex
	recoveryPending = map[appstate.WAPatchName]*pendingRecovery{}
)

// setEmitWhileRecovering keeps whatsmeow emitting events for recovered
// mutations while any request is open, so the label and quick reply stores
// refill whenever the reply lands. Call with recoveryMu held.
func setEmitWhileRecovering(client *whatsmeow.Client) {
	client.EmitAppStateEventsOnFullSync = len(recoveryPending) > 0
}

type appStateRecoveryResult struct {
	Collection        string `json:"collection"`
	VersionAfter      uint64 `json:"version_after"`
	LabelsRemoved     int64  `json:"labels_marked_removed"`
	ChatsRemoved      int64  `json:"associations_marked_removed"`
	QuickRepliesFound int    `json:"quick_replies"`
}

// handleAppStateRecoveryComplete closes an open request once whatsmeow has
// applied the phone's copy. For "regular", anything the copy no longer
// contains is marked removed, as after a full resync.
func handleAppStateRecoveryComplete(client *whatsmeow.Client, messageStore *MessageStore, evt *events.AppStateSyncComplete) {
	if !evt.Recovery {
		return
	}
	recoveryMu.Lock()
	p, ok := recoveryPending[evt.Name]
	if ok {
		p.timer.Stop()
		delete(recoveryPending, evt.Name)
		setEmitWhileRecovering(client)
	}
	recoveryMu.Unlock()

	result := appStateRecoveryResult{Collection: string(evt.Name), VersionAfter: evt.Version}
	if ok && evt.Name == appstate.WAPatchRegular {
		var err error
		if result.LabelsRemoved, result.ChatsRemoved, err = messageStore.markUnsyncedLabelsRemoved(p.sentAt); err != nil {
			fmt.Printf("App state recovery: failed to reconcile labels: %v\n", err)
		}
		if _, err = messageStore.markUnsyncedQuickRepliesRemoved(p.sentAt); err != nil {
			fmt.Printf("App state recovery: failed to reconcile quick replies: %v\n", err)
		}
		result.QuickRepliesFound, _ = messageStore.countQuickReplies()
	}
	b, _ := json.Marshal(result)
	fmt.Printf("App state recovery complete: %s\n", b)
}

// requestAppStateRecovery asks the phone for a copy of one collection and
// returns once the request is sent. whatsmeow applies the reply on its own.
func requestAppStateRecovery(ctx context.Context, client *whatsmeow.Client, name appstate.WAPatchName) error {
	recoveryMu.Lock()
	defer recoveryMu.Unlock()
	if _, busy := recoveryPending[name]; busy {
		return fmt.Errorf("a recovery of %s is already waiting for the phone", name)
	}
	p := &pendingRecovery{sentAt: time.Now()}
	p.timer = time.AfterFunc(appStateRecoveryWindow, func() {
		recoveryMu.Lock()
		defer recoveryMu.Unlock()
		if recoveryPending[name] == p {
			delete(recoveryPending, name)
			setEmitWhileRecovering(client)
			fmt.Printf("App state recovery of %s: no reply from the phone within %s\n", name, appStateRecoveryWindow)
		}
	})
	recoveryPending[name] = p
	setEmitWhileRecovering(client)

	if _, err := client.SendPeerMessage(ctx, whatsmeow.BuildAppStateRecoveryRequest(name)); err != nil {
		p.timer.Stop()
		delete(recoveryPending, name)
		setEmitWhileRecovering(client)
		return fmt.Errorf("failed to send recovery request: %w", err)
	}
	return nil
}

func registerAppStateRecoveryEndpoint(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc, client *whatsmeow.Client) {
	mux.HandleFunc("/api/app-state/recover", auth(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fail := func(status int, msg string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": msg})
		}

		if r.Method == http.MethodGet {
			recoveryMu.Lock()
			pending := map[string]string{}
			for n, p := range recoveryPending {
				pending[string(n)] = p.sentAt.Format(time.RFC3339)
			}
			recoveryMu.Unlock()
			versions := map[string]uint64{}
			for _, n := range appstate.AllPatchNames {
				v, _, _ := client.Store.AppState.GetAppStateVersion(r.Context(), string(n))
				versions[string(n)] = v
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "pending": pending, "versions": versions})
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Collection string `json:"collection"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fail(http.StatusBadRequest, "collection is required")
			return
		}
		name := appstate.WAPatchName(req.Collection)
		known := false
		for _, n := range appstate.AllPatchNames {
			known = known || n == name
		}
		if !known {
			fail(http.StatusBadRequest, fmt.Sprintf("unknown collection %q", req.Collection))
			return
		}
		fmt.Printf("→ /api/app-state/recover collection=%q from=%q\n", name, r.RemoteAddr)
		if !client.IsConnected() {
			fail(http.StatusServiceUnavailable, "Not connected to WhatsApp")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := requestAppStateRecovery(ctx, client, name); err != nil {
			fail(http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("Recovery request sent to the phone. It is applied when the phone replies (open WhatsApp on it); waiting up to %s. GET this endpoint to check.", appStateRecoveryWindow),
		})
	}))
}
