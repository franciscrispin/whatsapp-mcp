package main

// Writing app state the way WhatsApp's own clients do.
//
// A record's index MAC is derived from the app state key it was written with,
// so the same logical index (say ["contact", "<phone>"]) saved under two keys
// is two records. Official clients overwrite a record with a SET under the
// newest key plus a REMOVE of the copy under the old key. whatsmeow's
// SendAppState only sends the SET, which leaves the old copy in this bridge's
// state hash while every other device drops it: the hashes split and the next
// patch from another device fails with "mismatching LTHash" (whatsmeow issues
// #508, #518). This bridge broke its labels and contacts collections that way
// in October 2026.
//
// sendAppStateOverwrite encodes the patch itself so it can send that REMOVE,
// then pulls the patch back through whatsmeow's normal verified sync.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/cbcutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"
)

// appStateKeys is one expanded app state sync key.
type appStateKeys struct {
	id                                         []byte
	index, valueEnc, valueMAC, snapshot, patch []byte
}

func expandAppStateKey(id, data []byte) appStateKeys {
	e := hkdfutil.SHA256(data, nil, []byte("WhatsApp Mutation Keys"), 160)
	return appStateKeys{id: id, index: e[0:32], valueEnc: e[32:64], valueMAC: e[64:96], snapshot: e[96:128], patch: e[128:160]}
}

func hmacSHA256(key []byte, data ...[]byte) []byte {
	h := hmac.New(sha256.New, key)
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum(nil)
}

func uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// appStateWriteStore is what the encoder needs from whatsmeow's store.
type appStateWriteStore interface {
	GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error)
	GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error)
	GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error)
	GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error)
	// AllAppStateSyncKeyIDs lists every stored key; whatsmeow has no call for it.
	AllAppStateSyncKeyIDs(ctx context.Context) ([][]byte, error)
}

type whatsmeowWriteStore struct {
	device *store.Device
	waDB   *sql.DB
}

func (s whatsmeowWriteStore) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	return s.device.AppState.GetAppStateVersion(ctx, name)
}

func (s whatsmeowWriteStore) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	return s.device.AppState.GetAppStateMutationMAC(ctx, name, indexMAC)
}

func (s whatsmeowWriteStore) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	return s.device.AppStateKeys.GetAppStateSyncKey(ctx, id)
}

func (s whatsmeowWriteStore) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	return s.device.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
}

func (s whatsmeowWriteStore) AllAppStateSyncKeyIDs(ctx context.Context) ([][]byte, error) {
	if s.waDB == nil || s.device.ID == nil {
		return nil, fmt.Errorf("whatsmeow database is not open")
	}
	rows, err := s.waDB.QueryContext(ctx, `SELECT key_id FROM whatsmeow_app_state_sync_keys WHERE jid = ?`, s.device.ID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids [][]byte
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func encryptMutation(keys appStateKeys, op waServerSync.SyncdMutation_SyncdOperation, data *waSyncAction.SyncActionData) (*waServerSync.SyncdMutation, error) {
	content, err := proto.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mutation: %w", err)
	}
	encrypted, err := cbcutil.Encrypt(keys.valueEnc, nil, content)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt mutation: %w", err)
	}
	h := hmac.New(sha512.New, keys.valueMAC)
	h.Write([]byte{byte(op) + 1})
	h.Write(keys.id)
	h.Write(encrypted)
	h.Write(uint64Bytes(uint64(len(keys.id) + 1)))
	valueMAC := h.Sum(nil)[:32]
	return &waServerSync.SyncdMutation{
		Operation: op.Enum(),
		Record: &waServerSync.SyncdRecord{
			Index: &waServerSync.SyncdIndex{Blob: hmacSHA256(keys.index, data.GetIndex())},
			Value: &waServerSync.SyncdValue{Blob: append(encrypted, valueMAC...)},
			KeyID: &waServerSync.KeyId{ID: keys.id},
		},
	}, nil
}

// encodeAppStateOverwrite builds the patch for info, adding a REMOVE for every
// copy of each index stored under a key other than the newest. It returns the
// patch, the collection version it was built on and how many stale copies it
// removes.
func encodeAppStateOverwrite(ctx context.Context, st appStateWriteStore, info appstate.PatchInfo, now time.Time) (*waServerSync.SyncdPatch, uint64, int, error) {
	name := string(info.Type)
	version, hash, err := st.GetAppStateVersion(ctx, name)
	if err != nil {
		return nil, 0, 0, err
	}
	latestID, err := st.GetLatestAppStateSyncKeyID(ctx)
	if err != nil || latestID == nil {
		return nil, 0, 0, fmt.Errorf("no app state key to write with: %v", err)
	}
	keyIDs, err := st.AllAppStateSyncKeyIDs(ctx)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to list app state keys: %w", err)
	}
	expanded := map[string]appStateKeys{}
	expand := func(id []byte) (appStateKeys, error) {
		if k, ok := expanded[string(id)]; ok {
			return k, nil
		}
		raw, err := st.GetAppStateSyncKey(ctx, id)
		if err != nil || raw == nil {
			return appStateKeys{}, fmt.Errorf("app state key %X not found: %v", id, err)
		}
		k := expandAppStateKey(id, raw.Data)
		expanded[string(id)] = k
		return k, nil
	}
	latest, err := expand(latestID)
	if err != nil {
		return nil, 0, 0, err
	}

	ts := proto.Int64(now.UnixMilli())
	var mutations []*waServerSync.SyncdMutation
	stale := 0
	for _, m := range info.Mutations {
		indexBytes, err := json.Marshal(m.Index)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to marshal mutation index: %w", err)
		}
		actionVersion := m.Version
		for _, id := range keyIDs {
			if hmac.Equal(id, latestID) {
				continue
			}
			k, err := expand(id)
			if err != nil {
				continue // a key we cannot read cannot have written a record we know
			}
			prev, err := st.GetAppStateMutationMAC(ctx, name, hmacSHA256(k.index, indexBytes))
			if err != nil {
				return nil, 0, 0, err
			}
			if prev == nil {
				continue
			}
			rm, err := encryptMutation(k, waServerSync.SyncdMutation_REMOVE, &waSyncAction.SyncActionData{
				Index: indexBytes, Value: &waSyncAction.SyncActionValue{Timestamp: ts}, Padding: []byte{}, Version: &actionVersion,
			})
			if err != nil {
				return nil, 0, 0, err
			}
			mutations = append(mutations, rm)
			stale++
		}
		value := proto.Clone(m.Value).(*waSyncAction.SyncActionValue)
		value.Timestamp = ts
		set, err := encryptMutation(latest, waServerSync.SyncdMutation_SET, &waSyncAction.SyncActionData{
			Index: indexBytes, Value: value, Padding: []byte{}, Version: &actionVersion,
		})
		if err != nil {
			return nil, 0, 0, err
		}
		mutations = append(mutations, set)
	}
	patch, err := finishAppStatePatch(ctx, st, name, version, hash, latest, mutations)
	return patch, version, stale, err
}

// encodeAppStateStaleRemoval builds a patch that only removes the copy of index
// written under keyID, for a copy the server still holds but this device and
// the others already dropped (left by an old SET-only write). Nobody holds it,
// so nobody's hash changes; the server stops handing it out in snapshots,
// which otherwise fail to verify on every full sync.
func encodeAppStateStaleRemoval(ctx context.Context, st appStateWriteStore, name appstate.WAPatchName, keyID []byte, index []string, actionVersion int32, now time.Time) (*waServerSync.SyncdPatch, uint64, error) {
	version, hash, err := st.GetAppStateVersion(ctx, string(name))
	if err != nil {
		return nil, 0, err
	}
	latestID, err := st.GetLatestAppStateSyncKeyID(ctx)
	if err != nil || latestID == nil {
		return nil, 0, fmt.Errorf("no app state key to write with: %v", err)
	}
	keys := map[string]appStateKeys{}
	for _, id := range [][]byte{keyID, latestID} {
		raw, err := st.GetAppStateSyncKey(ctx, id)
		if err != nil || raw == nil {
			return nil, 0, fmt.Errorf("app state key %X not found: %v", id, err)
		}
		keys[string(id)] = expandAppStateKey(id, raw.Data)
	}
	indexBytes, err := json.Marshal(index)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal mutation index: %w", err)
	}
	old := keys[string(keyID)]
	if prev, err := st.GetAppStateMutationMAC(ctx, string(name), hmacSHA256(old.index, indexBytes)); err != nil {
		return nil, 0, err
	} else if prev != nil {
		return nil, 0, fmt.Errorf("this device still holds that copy; overwrite the record instead")
	}
	rm, err := encryptMutation(old, waServerSync.SyncdMutation_REMOVE, &waSyncAction.SyncActionData{
		Index: indexBytes, Value: &waSyncAction.SyncActionValue{Timestamp: proto.Int64(now.UnixMilli())}, Padding: []byte{}, Version: &actionVersion,
	})
	if err != nil {
		return nil, 0, err
	}
	patch, err := finishAppStatePatch(ctx, st, string(name), version, hash, keys[string(latestID)], []*waServerSync.SyncdMutation{rm})
	return patch, version, err
}

// finishAppStatePatch computes the state hash after mutations and the patch's
// snapshot and patch MACs, signing with the newest key.
func finishAppStatePatch(ctx context.Context, st appStateWriteStore, name string, version uint64, hash [128]byte, latest appStateKeys, mutations []*waServerSync.SyncdMutation) (*waServerSync.SyncdPatch, error) {
	var err error
	// Same hash rules whatsmeow applies when it validates the patch on the way back.
	var added, removed [][]byte
	for i, m := range mutations {
		blob := m.GetRecord().GetValue().GetBlob()
		if m.GetOperation() == waServerSync.SyncdMutation_SET {
			added = append(added, blob[len(blob)-32:])
		}
		indexMAC := m.GetRecord().GetIndex().GetBlob()
		var prev []byte
		inPatch := false
		for j := i - 1; j >= 0; j-- {
			if hmac.Equal(mutations[j].GetRecord().GetIndex().GetBlob(), indexMAC) {
				inPatch = true
				if mutations[j].GetOperation() == waServerSync.SyncdMutation_SET {
					b := mutations[j].GetRecord().GetValue().GetBlob()
					prev = b[len(b)-32:]
				}
				break
			}
		}
		if !inPatch {
			if prev, err = st.GetAppStateMutationMAC(ctx, name, indexMAC); err != nil {
				return nil, err
			}
		}
		if prev != nil {
			removed = append(removed, prev)
		}
	}
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(hash[:], removed, added)

	newVersion := version + 1
	patch := &waServerSync.SyncdPatch{
		SnapshotMAC: hmacSHA256(latest.snapshot, hash[:], uint64Bytes(newVersion), []byte(name)),
		KeyID:       &waServerSync.KeyId{ID: latest.id},
		Mutations:   mutations,
	}
	macInput := [][]byte{patch.SnapshotMAC}
	for _, m := range mutations {
		blob := m.GetRecord().GetValue().GetBlob()
		macInput = append(macInput, blob[len(blob)-32:])
	}
	macInput = append(macInput, uint64Bytes(newVersion), []byte(name))
	patch.PatchMAC = hmacSHA256(latest.patch, macInput...)
	return patch, nil
}

// sendAppStateOverwrite sends info as one patch with old-key copies removed,
// then syncs the collection so whatsmeow verifies and stores the result. On a
// version conflict it syncs first and retries once.
func sendAppStateOverwrite(ctx context.Context, client *whatsmeow.Client, waDB *sql.DB, info appstate.PatchInfo) error {
	st := whatsmeowWriteStore{device: client.Store, waDB: waDB}
	for attempt := 0; ; attempt++ {
		patch, version, stale, err := encodeAppStateOverwrite(ctx, st, info, time.Now())
		if err != nil {
			return err
		}
		conflict, err := sendAppStatePatch(ctx, client, info.Type, version, patch)
		if conflict && attempt == 0 {
			// Someone else wrote first: catch up, then rebuild on the new state.
			if err := client.FetchAppState(ctx, info.Type, false, false); err != nil {
				return fmt.Errorf("app state conflict, and catching up failed: %w", err)
			}
			continue
		} else if err != nil {
			return err
		}
		if stale > 0 {
			fmt.Printf("App state write to %s removed %d old-key copies\n", info.Type, stale)
		}
		return client.FetchAppState(ctx, info.Type, false, false)
	}
}

// sendAppStatePatch uploads one encoded patch built on version. conflict is
// true when WhatsApp refused it because the collection has moved on (409).
func sendAppStatePatch(ctx context.Context, client *whatsmeow.Client, name appstate.WAPatchName, version uint64, patch *waServerSync.SyncdPatch) (conflict bool, err error) {
	encoded, err := proto.Marshal(patch)
	if err != nil {
		return false, fmt.Errorf("failed to marshal patch: %w", err)
	}
	resp, err := client.DangerousInternals().SendIQ(ctx, whatsmeow.DangerousInfoQuery{
		Namespace: "w:sync:app:state",
		Type:      "set",
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "sync",
			Content: []waBinary.Node{{
				Tag:     "collection",
				Attrs:   waBinary.Attrs{"name": string(name), "version": version, "return_snapshot": false},
				Content: []waBinary.Node{{Tag: "patch", Content: encoded}},
			}},
		}},
	})
	if err != nil {
		return false, err
	}
	coll, ok := resp.GetOptionalChildByTag("sync", "collection")
	if !ok {
		return false, fmt.Errorf("app state response has no collection")
	}
	if coll.AttrGetter().OptionalString("type") == "error" {
		errTag, _ := coll.GetOptionalChildByTag("error")
		err = fmt.Errorf("WhatsApp rejected the %s patch: %s", name, &errTag)
		return errTag.AttrGetter().OptionalInt("code") == 409, err
	}
	return false, nil
}
