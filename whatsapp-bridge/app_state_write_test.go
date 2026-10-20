package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type appStateFixture struct {
	ctx    context.Context
	device *store.Device
	st     whatsmeowWriteStore
	proc   *appstate.Processor
}

func newAppStateFixture(t *testing.T) *appStateFixture {
	t.Helper()
	ctx := context.Background()
	dsn := "file:" + t.Name() + "?mode=memory&cache=shared&_foreign_keys=on"
	container, err := sqlstore.New(ctx, "sqlite3", dsn, waLog.Noop)
	if err != nil {
		t.Fatal(err)
	}
	device := container.NewDevice()
	jid := types.NewADJID("6580246640", 0, 39)
	device.ID = &jid
	// Placeholder identity sized to satisfy the device table's checks.
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte{}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	if err := container.PutDevice(ctx, device); err != nil {
		t.Fatal(err)
	}
	waDB, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = waDB.Close(); _ = container.Close() })
	return &appStateFixture{ctx: ctx, device: device, st: whatsmeowWriteStore{device: device, waDB: waDB}, proc: appstate.NewProcessor(device, waLog.Noop)}
}

func (f *appStateFixture) addKey(t *testing.T, id byte, ts int64) {
	t.Helper()
	data := make([]byte, 32)
	data[0] = id
	err := f.device.AppStateKeys.PutAppStateSyncKey(f.ctx, []byte{0, 0, 0, 0, 0x46, id}, store.AppStateSyncKey{Data: data, Fingerprint: []byte{1}, Timestamp: ts})
	if err != nil {
		t.Fatal(err)
	}
}

// write encodes info with the overwrite encoder, then applies it exactly as
// whatsmeow does when it syncs the patch back, failing on any MAC mismatch.
func (f *appStateFixture) write(t *testing.T, info appstate.PatchInfo) int {
	t.Helper()
	patch, version, stale, err := encodeAppStateOverwrite(f.ctx, f.st, info, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(version + 1)}
	_, hash, _ := f.device.AppState.GetAppStateVersion(f.ctx, string(info.Type))
	list := &appstate.PatchList{Name: info.Type, Patches: []*waServerSync.SyncdPatch{patch}}
	if _, _, err := f.proc.DecodePatches(f.ctx, list, appstate.HashState{Version: version, Hash: hash}, true); err != nil {
		t.Fatalf("whatsmeow rejected the encoded patch: %v", err)
	}
	return stale
}

func contactPatch(name string) appstate.PatchInfo {
	return appstate.PatchInfo{
		Type: appstate.WAPatchCriticalUnblockLow,
		Mutations: []appstate.MutationInfo{{
			Index:   []string{"contact", "6591234567@s.whatsapp.net"},
			Version: 2,
			Value:   &waSyncAction.SyncActionValue{ContactAction: &waSyncAction.ContactAction{FullName: proto.String(name)}},
		}},
	}
}

// countMACs counts distinct records; whatsmeow keeps a row per version of each.
func countMACs(t *testing.T, f *appStateFixture, name appstate.WAPatchName) int {
	t.Helper()
	var n int
	if err := f.st.waDB.QueryRow(`SELECT COUNT(DISTINCT index_mac) FROM whatsmeow_app_state_mutation_macs WHERE name = ?`, string(name)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The record was first saved under an older key. Overwriting it under the
// newest key must remove the old copy, so the state holds one record and its
// hash is exactly that record's.
func TestOverwriteRemovesOldKeyCopy(t *testing.T) {
	f := newAppStateFixture(t)
	name := appstate.WAPatchCriticalUnblockLow

	f.addKey(t, 1, 1000)
	if stale := f.write(t, contactPatch("Ann")); stale != 0 {
		t.Fatalf("first write removed %d copies, want 0", stale)
	}

	f.addKey(t, 2, 2000) // a newer key arrives, as happens most days
	if stale := f.write(t, contactPatch("Ann Tan")); stale != 1 {
		t.Fatalf("overwrite removed %d old-key copies, want 1", stale)
	}
	if n := countMACs(t, f, name); n != 1 {
		t.Fatalf("%d records stored for one contact, want 1", n)
	}

	var valueMAC []byte
	if err := f.st.waDB.QueryRow(`SELECT value_mac FROM whatsmeow_app_state_mutation_macs WHERE name = ? ORDER BY version DESC LIMIT 1`, string(name)).Scan(&valueMAC); err != nil {
		t.Fatal(err)
	}
	var want [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(want[:], nil, [][]byte{valueMAC})
	version, got, _ := f.device.AppState.GetAppStateVersion(f.ctx, string(name))
	if got != want || version != 2 {
		t.Fatal("state hash is not the hash of the single remaining record")
	}

	// A further overwrite under the same newest key needs no REMOVE.
	if stale := f.write(t, contactPatch("Ann T")); stale != 0 {
		t.Fatalf("same-key overwrite removed %d copies, want 0", stale)
	}
	if n := countMACs(t, f, name); n != 1 {
		t.Fatalf("%d records after same-key overwrite, want 1", n)
	}
}

// Removing a copy only the server still holds verifies through whatsmeow and
// leaves the state hash and the stored records exactly as they were.
func TestStaleRemovalLeavesStateUnchanged(t *testing.T) {
	f := newAppStateFixture(t)
	name := appstate.WAPatchCriticalUnblockLow
	f.addKey(t, 1, 1000)
	f.addKey(t, 2, 2000)
	f.write(t, contactPatch("Ann")) // held under key 2 only
	_, before, _ := f.device.AppState.GetAppStateVersion(f.ctx, string(name))

	staleKey := []byte{0, 0, 0, 0, 0x46, 1}
	patch, version, err := encodeAppStateStaleRemoval(f.ctx, f.st, name, staleKey, contactPatch("").Mutations[0].Index, 2, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(version + 1)}
	list := &appstate.PatchList{Name: name, Patches: []*waServerSync.SyncdPatch{patch}}
	if _, _, err := f.proc.DecodePatches(f.ctx, list, appstate.HashState{Version: version, Hash: before}, true); err != nil {
		t.Fatalf("whatsmeow rejected the removal: %v", err)
	}
	after, hash, _ := f.device.AppState.GetAppStateVersion(f.ctx, string(name))
	if hash != before || after != version+1 {
		t.Fatal("removing a copy nobody holds changed the state hash")
	}
	if n := countMACs(t, f, name); n != 1 {
		t.Fatalf("%d records after removal, want the 1 current record", n)
	}

	// The current copy is still held, so it must be overwritten, not removed.
	if _, _, err := encodeAppStateStaleRemoval(f.ctx, f.st, name, []byte{0, 0, 0, 0, 0x46, 2}, contactPatch("").Mutations[0].Index, 2, time.Now()); err == nil {
		t.Fatal("removal of a held copy was not refused")
	}
}

// whatsmeow's own encoder leaves the old copy behind: the bug this file fixes.
func TestWhatsmeowEncoderLeavesOldKeyCopy(t *testing.T) {
	f := newAppStateFixture(t)
	name := appstate.WAPatchCriticalUnblockLow
	f.addKey(t, 1, 1000)
	f.write(t, contactPatch("Ann"))
	f.addKey(t, 2, 2000)

	version, hash, _ := f.device.AppState.GetAppStateVersion(f.ctx, string(name))
	raw, err := f.proc.EncodePatch(f.ctx, []byte{0, 0, 0, 0, 0x46, 2}, appstate.HashState{Version: version, Hash: hash}, contactPatch("Ann Tan"))
	if err != nil {
		t.Fatal(err)
	}
	var patch waServerSync.SyncdPatch
	if err := proto.Unmarshal(raw, &patch); err != nil {
		t.Fatal(err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(version + 1)}
	list := &appstate.PatchList{Name: name, Patches: []*waServerSync.SyncdPatch{&patch}}
	if _, _, err := f.proc.DecodePatches(f.ctx, list, appstate.HashState{Version: version, Hash: hash}, true); err != nil {
		t.Fatal(err)
	}
	if n := countMACs(t, f, name); n != 2 {
		t.Fatalf("whatsmeow encoder left %d records, expected the 2 that cause the split", n)
	}
}
