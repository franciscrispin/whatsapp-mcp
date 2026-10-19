package main

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

type fakePNs map[string]string // phone -> LID

func (f fakePNs) GetLIDForPN(_ context.Context, pn types.JID) (types.JID, error) {
	if lid, ok := f[pn.User]; ok {
		return types.NewJID(lid, types.HiddenUserServer), nil
	}
	return types.JID{}, nil
}

func (f fakePNs) GetPNForLID(_ context.Context, lid types.JID) (types.JID, error) {
	for pn, l := range f {
		if l == lid.User {
			return types.NewJID(pn, types.DefaultUserServer), nil
		}
	}
	return types.JID{}, nil
}

func TestResolveContactTarget(t *testing.T) {
	lids := fakePNs{"6591234567": "111"}
	cases := []struct{ in, pn, lid string }{
		{"6591234567", "6591234567@s.whatsapp.net", "111@lid"},
		{"+6591234567", "6591234567@s.whatsapp.net", "111@lid"},
		{"6591234567:12@s.whatsapp.net", "6591234567@s.whatsapp.net", "111@lid"},
		{"111@lid", "6591234567@s.whatsapp.net", "111@lid"},
		// A phone with no known LID is still a valid contact key.
		{"6599999999", "6599999999@s.whatsapp.net", ""},
	}
	for _, c := range cases {
		pn, lid, err := resolveContactTarget(context.Background(), lids, c.in)
		gotLID := ""
		if !lid.IsEmpty() {
			gotLID = lid.String()
		}
		if err != nil || pn.String() != c.pn || gotLID != c.lid {
			t.Errorf("resolveContactTarget(%q) = %v, %q, %v; want %s, %q", c.in, pn, gotLID, err, c.pn, c.lid)
		}
	}
}

func TestResolveContactTargetRejects(t *testing.T) {
	lids := fakePNs{}
	if _, _, err := resolveContactTarget(context.Background(), lids, "222@lid"); !errors.Is(err, errNoPhoneForLID) {
		t.Errorf("unmapped LID: got %v, want errNoPhoneForLID", err)
	}
	for _, in := range []string{"", "120363000000000001@g.us", "status@broadcast", "123@newsletter"} {
		if _, _, err := resolveContactTarget(context.Background(), lids, in); err == nil {
			t.Errorf("resolveContactTarget(%q) succeeded, want an error", in)
		}
	}
}

// The patch must never ask the phone to write to its own address book.
func TestBuildContactRename(t *testing.T) {
	pn := types.NewJID("6591234567", types.DefaultUserServer)
	lid := types.NewJID("111", types.HiddenUserServer)
	patch := buildContactRename(pn, lid, "Ann Tan", "Ann")
	if patch.Type != appstate.WAPatchCriticalUnblockLow || len(patch.Mutations) != 1 {
		t.Fatalf("unexpected patch: %+v", patch)
	}
	m := patch.Mutations[0]
	if len(m.Index) != 2 || m.Index[0] != "contact" || m.Index[1] != "6591234567@s.whatsapp.net" || m.Version != 2 {
		t.Errorf("index/version = %v/%d", m.Index, m.Version)
	}
	act := m.Value.GetContactAction()
	if act.GetFullName() != "Ann Tan" || act.GetFirstName() != "Ann" || act.GetLidJID() != "111@lid" {
		t.Errorf("action = %v", act)
	}
	if act.SaveOnPrimaryAddressbook == nil || act.GetSaveOnPrimaryAddressbook() {
		t.Error("saveOnPrimaryAddressbook must be sent as false")
	}

	bare := buildContactRename(pn, types.JID{}, "Ann Tan", "").Mutations[0].Value.GetContactAction()
	if bare.FirstName != nil || bare.LidJID != nil {
		t.Errorf("empty first name and LID should be omitted: %v", bare)
	}
}
