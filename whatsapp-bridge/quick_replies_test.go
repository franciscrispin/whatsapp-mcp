package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func newTestQuickReplyStore(t *testing.T) *MessageStore {
	t.Helper()
	ms := newTestMessageStore(t)
	for i := 0; i < 2; i++ { // runs on every bridge start, so twice must be harmless
		if err := ensureQuickReplySchema(ms.db); err != nil {
			t.Fatalf("ensureQuickReplySchema: %v", err)
		}
	}
	return ms
}

func quickReply(id, shortcut, message string, deleted bool) *events.AppState {
	return &events.AppState{
		Index: []string{"quick_reply", id},
		SyncActionValue: &waSyncAction.SyncActionValue{
			Timestamp: proto.Int64(1760000000000),
			QuickReplyAction: &waSyncAction.QuickReplyAction{
				Shortcut: proto.String(shortcut),
				Message:  proto.String(message),
				Keywords: []string{"deposit"},
				Count:    proto.Int32(3),
				Deleted:  proto.Bool(deleted),
			},
		},
	}
}

type quickReplyRow struct {
	shortcut, message, keywords, labelIDs string
	count                                 int
	deleted                               bool
	updatedAt                             time.Time
}

func readQuickReply(t *testing.T, ms *MessageStore, id string) quickReplyRow {
	t.Helper()
	var r quickReplyRow
	err := ms.db.QueryRow(`SELECT shortcut, message, keywords, label_ids, use_count, deleted, updated_at
		FROM quick_replies WHERE quick_reply_id = ?`, id).
		Scan(&r.shortcut, &r.message, &r.keywords, &r.labelIDs, &r.count, &r.deleted, &r.updatedAt)
	if err != nil {
		t.Fatalf("read quick reply %s: %v", id, err)
	}
	return r
}

// An edit arrives as a fresh SET under the same id and must overwrite the row.
func TestStoreQuickReplyEditOverwrites(t *testing.T) {
	ms := newTestQuickReplyStore(t)
	handleQuickReplyEvent(ms, quickReply("1", "dep", "Deposit is 30%", false))
	handleQuickReplyEvent(ms, quickReply("1", "deposit", "Deposit is 25%", false))

	r := readQuickReply(t, ms, "1")
	if r.shortcut != "deposit" || r.message != "Deposit is 25%" || r.deleted {
		t.Errorf("row after edit = %+v", r)
	}
	if r.keywords != `["deposit"]` || r.labelIDs != `[]` || r.count != 3 {
		t.Errorf("keywords/labels/count = %s %s %d", r.keywords, r.labelIDs, r.count)
	}
	if !r.updatedAt.Equal(time.UnixMilli(1760000000000)) {
		t.Errorf("updated_at = %v, want the action timestamp", r.updatedAt)
	}
	if n, _ := ms.countQuickReplies(); n != 1 {
		t.Errorf("countQuickReplies = %d, want 1", n)
	}
}

func TestStoreQuickReplyDeletedFlag(t *testing.T) {
	ms := newTestQuickReplyStore(t)
	handleQuickReplyEvent(ms, quickReply("1", "dep", "x", false))
	handleQuickReplyEvent(ms, quickReply("1", "dep", "x", true))
	if !readQuickReply(t, ms, "1").deleted {
		t.Error("deleted flag not stored")
	}
	if n, _ := ms.countQuickReplies(); n != 0 {
		t.Errorf("countQuickReplies = %d, want 0", n)
	}
}

// Every app state mutation arrives as events.AppState; only quick replies count.
func TestStoreQuickReplyIgnoresOtherIndexes(t *testing.T) {
	ms := newTestQuickReplyStore(t)
	other := []*events.AppState{
		{Index: []string{"label_edit", "7"}, SyncActionValue: &waSyncAction.SyncActionValue{}},
		{Index: []string{"quick_reply"}, SyncActionValue: &waSyncAction.SyncActionValue{}},
		{Index: []string{"quick_reply", "2"}, SyncActionValue: &waSyncAction.SyncActionValue{}},
		{Index: []string{"quick_reply", "3"}},
	}
	for _, e := range other {
		if stored, err := ms.StoreQuickReply(e); stored || err != nil {
			t.Errorf("StoreQuickReply(%v) = %v, %v; want ignored", e.Index, stored, err)
		}
	}
}

func TestMarkUnsyncedQuickRepliesRemoved(t *testing.T) {
	ms := newTestQuickReplyStore(t)
	handleQuickReplyEvent(ms, quickReply("old", "a", "x", false))
	cutoff := time.Now()
	time.Sleep(time.Millisecond)
	handleQuickReplyEvent(ms, quickReply("new", "b", "y", false))

	n, err := ms.markUnsyncedQuickRepliesRemoved(cutoff)
	if err != nil || n != 1 {
		t.Fatalf("markUnsyncedQuickRepliesRemoved = %d, %v; want 1", n, err)
	}
	if !readQuickReply(t, ms, "old").deleted || readQuickReply(t, ms, "new").deleted {
		t.Error("only the reply missing from the replay should be marked deleted")
	}
}
