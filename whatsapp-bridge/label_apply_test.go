package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

type fakeLIDs map[string]string

func (f fakeLIDs) GetLIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	if lid, ok := f[pn.User]; ok {
		return types.NewJID(lid, types.HiddenUserServer), nil
	}
	return types.JID{}, nil
}

func TestResolveLabelTarget(t *testing.T) {
	lids := fakeLIDs{"6591234567": "111"}
	cases := []struct{ in, want string }{
		{"6591234567", "111@lid"},
		{"+6591234567", "111@lid"},
		{"6591234567@s.whatsapp.net", "111@lid"},
		{"6591234567:12@s.whatsapp.net", "111@lid"},
		{"111@lid", "111@lid"},
		{"120363000000000001@g.us", "120363000000000001@g.us"},
	}
	for _, c := range cases {
		got, err := resolveLabelTarget(context.Background(), lids, c.in)
		if err != nil || got.String() != c.want {
			t.Errorf("resolveLabelTarget(%q) = %v, %v; want %s", c.in, got, err, c.want)
		}
	}
}

// A phone number with no known LID must fail rather than label the phone
// JID: the phone app would not show an association keyed that way.
func TestResolveLabelTargetRejects(t *testing.T) {
	lids := fakeLIDs{}
	if _, err := resolveLabelTarget(context.Background(), lids, "6599999999"); !errors.Is(err, errNoLIDForPhone) {
		t.Errorf("unmapped phone: got %v, want errNoLIDForPhone", err)
	}
	for _, in := range []string{"", "status@broadcast", "123@newsletter"} {
		if _, err := resolveLabelTarget(context.Background(), lids, in); err == nil {
			t.Errorf("resolveLabelTarget(%q) succeeded, want an error", in)
		}
	}
}

func TestRateLimiter(t *testing.T) {
	l := &rateLimiter{limit: 2, window: time.Minute}
	now := time.Now()
	if !l.allow(now) || !l.allow(now.Add(time.Second)) {
		t.Fatal("first two calls should be allowed")
	}
	if l.allow(now.Add(2 * time.Second)) {
		t.Fatal("third call inside the window should be refused")
	}
	if !l.allow(now.Add(61 * time.Second)) {
		t.Fatal("a call after the window should be allowed again")
	}
}

func TestLiveLabelExistsAndChatHasLabel(t *testing.T) {
	ms := newTestLabelStore(t)
	for _, e := range []interface{}{labelEdit("7", "TZ", 18, false), labelEdit("6", "Old", 2, true), labelChat("7", "111", true)} {
		handleLabelEvent(ms, e)
	}
	for id, want := range map[string]bool{"7": true, "6": false, "99": false} {
		if got, err := ms.liveLabelExists(id); err != nil || got != want {
			t.Errorf("liveLabelExists(%s) = %v, %v; want %v", id, got, err, want)
		}
	}
	if got, _ := ms.chatHasLabel("7", "111@lid"); !got {
		t.Error("chatHasLabel(7, 111@lid) = false, want true")
	}
	if got, _ := ms.chatHasLabel("7", "222@lid"); got {
		t.Error("chatHasLabel(7, 222@lid) = true, want false")
	}
}
