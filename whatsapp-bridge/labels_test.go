package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func newTestLabelStore(t *testing.T) *MessageStore {
	t.Helper()
	ms := newTestMessageStore(t)
	if err := ensureLabelSchema(ms.db); err != nil {
		t.Fatalf("ensureLabelSchema: %v", err)
	}
	// Running it twice must be harmless: it runs on every bridge start.
	if err := ensureLabelSchema(ms.db); err != nil {
		t.Fatalf("ensureLabelSchema (second run): %v", err)
	}
	return ms
}

func labelEdit(id, name string, color int32, deleted bool) *events.LabelEdit {
	return &events.LabelEdit{
		LabelID:   id,
		Timestamp: time.Now(),
		Action: &waSyncAction.LabelEditAction{
			Name: proto.String(name), Color: proto.Int32(color), Deleted: proto.Bool(deleted),
		},
	}
}

func labelChat(id, jid string, labeled bool) *events.LabelAssociationChat {
	return &events.LabelAssociationChat{
		LabelID:   id,
		JID:       types.NewJID(jid, types.HiddenUserServer),
		Timestamp: time.Now(),
		Action:    &waSyncAction.LabelAssociationAction{Labeled: proto.Bool(labeled)},
	}
}

func TestStoreLabelEditUpserts(t *testing.T) {
	ms := newTestLabelStore(t)
	if err := ms.StoreLabelEdit(labelEdit("7", "TZ", 18, false)); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreLabelEdit(labelEdit("7", "Tanzania", 4, false)); err != nil {
		t.Fatal(err)
	}
	var name string
	var color, n int
	if err := ms.db.QueryRow(`SELECT name, color, (SELECT COUNT(*) FROM labels) FROM labels WHERE label_id = '7'`).Scan(&name, &color, &n); err != nil {
		t.Fatal(err)
	}
	if name != "Tanzania" || color != 4 || n != 1 {
		t.Fatalf("got name=%q color=%d rows=%d, want Tanzania/4/1", name, color, n)
	}
}

func TestStoreLabelChatKeepsRemovedRows(t *testing.T) {
	ms := newTestLabelStore(t)
	if err := ms.StoreLabelChat(labelChat("7", "111", true)); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreLabelChat(labelChat("7", "111", false)); err != nil {
		t.Fatal(err)
	}
	var jid string
	var labeled, n int
	if err := ms.db.QueryRow(`SELECT chat_jid, labeled, (SELECT COUNT(*) FROM label_chats) FROM label_chats`).Scan(&jid, &labeled, &n); err != nil {
		t.Fatal(err)
	}
	if jid != "111@lid" || labeled != 0 || n != 1 {
		t.Fatalf("got jid=%q labeled=%d rows=%d, want 111@lid/0/1", jid, labeled, n)
	}
}

// A full resync replays current state; anything it does not mention was
// removed by a REMOVE mutation, which whatsmeow never dispatches.
func TestMarkUnsyncedLabelsRemoved(t *testing.T) {
	ms := newTestLabelStore(t)
	for _, e := range []*events.LabelEdit{labelEdit("7", "TZ", 18, false), labelEdit("12", "Old", 1, false)} {
		if err := ms.StoreLabelEdit(e); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []*events.LabelAssociationChat{labelChat("7", "111", true), labelChat("7", "222", true)} {
		if err := ms.StoreLabelChat(e); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	// The "resync" only replays label 7 and chat 111.
	if err := ms.StoreLabelEdit(labelEdit("7", "TZ", 18, false)); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreLabelChat(labelChat("7", "111", true)); err != nil {
		t.Fatal(err)
	}

	labels, chats, err := ms.markUnsyncedLabelsRemoved(start)
	if err != nil {
		t.Fatal(err)
	}
	if labels != 1 || chats != 1 {
		t.Fatalf("marked labels=%d chats=%d, want 1/1", labels, chats)
	}
	gotLabels, gotChats, err := ms.countLabels()
	if err != nil {
		t.Fatal(err)
	}
	if gotLabels != 1 || gotChats != 1 {
		t.Fatalf("live labels=%d chats=%d, want 1/1", gotLabels, gotChats)
	}
	var deleted int
	if err := ms.db.QueryRow(`SELECT deleted FROM labels WHERE label_id = '12'`).Scan(&deleted); err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("label 12 deleted=%d, want 1 (row kept, flagged)", deleted)
	}
}

func TestHandleLabelEventIgnoresOtherEvents(t *testing.T) {
	ms := newTestLabelStore(t)
	handleLabelEvent(ms, &events.Connected{})
	handleLabelEvent(ms, labelEdit("3", "KG", 11, false))
	labels, _, err := ms.countLabels()
	if err != nil {
		t.Fatal(err)
	}
	if labels != 1 {
		t.Fatalf("labels=%d, want 1", labels)
	}
}
