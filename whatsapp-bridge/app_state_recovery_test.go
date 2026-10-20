package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types/events"
)

// A recovery reply for "regular" closes the request and marks labels and quick
// replies the phone's copy did not mention as removed.
func TestHandleAppStateRecoveryComplete(t *testing.T) {
	ms := newTestQuickReplyStore(t)
	if err := ensureLabelSchema(ms.db); err != nil {
		t.Fatal(err)
	}
	handleQuickReplyEvent(ms, quickReply("stale", "a", "x", false))
	time.Sleep(time.Millisecond)

	client := &whatsmeow.Client{}
	p := &pendingRecovery{sentAt: time.Now(), timer: time.NewTimer(time.Hour)}
	recoveryPending["regular"] = p
	setEmitWhileRecovering(client)
	if !client.EmitAppStateEventsOnFullSync {
		t.Fatal("events must be emitted while a recovery is open")
	}
	handleQuickReplyEvent(ms, quickReply("fresh", "b", "y", false)) // arrives with the recovery

	// A normal sync completion must not close the request.
	handleAppStateRecoveryComplete(client, ms, &events.AppStateSyncComplete{Name: "regular", Version: 9})
	if _, ok := recoveryPending["regular"]; !ok {
		t.Fatal("non-recovery completion closed the request")
	}

	handleAppStateRecoveryComplete(client, ms, &events.AppStateSyncComplete{Name: "regular", Version: 9, Recovery: true})
	if _, ok := recoveryPending["regular"]; ok || client.EmitAppStateEventsOnFullSync {
		t.Fatal("recovery reply should close the request and stop emitting")
	}
	if !readQuickReply(t, ms, "stale").deleted || readQuickReply(t, ms, "fresh").deleted {
		t.Error("only the reply missing from the recovered copy should be marked deleted")
	}
}
