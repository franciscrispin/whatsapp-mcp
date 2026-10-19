package main

// Renaming one saved contact.
//
// whatsmeow reads contact names from the critical_unblock_low app state
// collection but has no builder to write one, so the patch is built here the
// same way appstate.BuildLabelChat builds a label association. The
// saveOnPrimaryAddressbook flag is always sent as false: setting it would ask
// the phone to write the name into its own address book, and that is not a
// change this bridge should be able to make. Writes share the label rate limit
// so a runaway caller stalls instead of renaming a whole contact list.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// contactMutationVersion is the action version WhatsApp clients send for a
// contact edit.
const contactMutationVersion = 2

// ContactRenameRequest is the request body for /api/contacts/rename.
type ContactRenameRequest struct {
	Phone     string `json:"phone"`      // phone number, phone JID or LID
	FullName  string `json:"full_name"`  // required
	FirstName string `json:"first_name"` // optional; empty clears it
	DryRun    bool   `json:"dry_run"`    // build and return the patch without sending it
}

// pnLookup is the part of whatsmeow's LID store needed to key a contact.
type pnLookup interface {
	GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error)
	GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error)
}

var errNoPhoneForLID = errors.New("no phone number known for this LID")

// resolveContactTarget turns caller input into the phone JID contacts are
// keyed by, plus the matching LID when one is known locally. Nothing is
// looked up on WhatsApp's side.
func resolveContactTarget(ctx context.Context, lids pnLookup, raw string) (pn, lid types.JID, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return pn, lid, errors.New("phone is required")
	}
	var jid types.JID
	if strings.Contains(raw, "@") {
		parsed, perr := types.ParseJID(raw)
		if perr != nil {
			return pn, lid, fmt.Errorf("invalid phone: %w", perr)
		}
		jid = parsed.ToNonAD()
	} else {
		jid = types.NewJID(strings.TrimPrefix(raw, "+"), types.DefaultUserServer)
	}

	switch jid.Server {
	case types.DefaultUserServer:
		pn = jid
		if lid, err = lids.GetLIDForPN(ctx, pn); err != nil {
			return pn, lid, fmt.Errorf("LID lookup failed for %s: %w", pn, err)
		}
		return pn, lid, nil
	case types.HiddenUserServer:
		lid = jid
		if pn, err = lids.GetPNForLID(ctx, lid); err != nil {
			return pn, lid, fmt.Errorf("phone lookup failed for %s: %w", lid, err)
		}
		if pn.IsEmpty() {
			return pn, lid, fmt.Errorf("%w: %s", errNoPhoneForLID, lid.User)
		}
		return pn, lid, nil
	default:
		return pn, lid, fmt.Errorf("cannot rename a %s contact", jid.Server)
	}
}

// buildContactRename builds the app state patch that sets a contact's name.
func buildContactRename(pn, lid types.JID, fullName, firstName string) appstate.PatchInfo {
	action := &waSyncAction.ContactAction{
		FullName:                 proto.String(fullName),
		SaveOnPrimaryAddressbook: proto.Bool(false),
	}
	if firstName != "" {
		action.FirstName = proto.String(firstName)
	}
	if !lid.IsEmpty() {
		action.LidJID = proto.String(lid.String())
	}
	return appstate.PatchInfo{
		Type: appstate.WAPatchCriticalUnblockLow,
		Mutations: []appstate.MutationInfo{{
			Index:   []string{appstate.IndexContact, pn.String()},
			Version: contactMutationVersion,
			Value:   &waSyncAction.SyncActionValue{ContactAction: action},
		}},
	}
}

func registerContactRenameEndpoint(mux *http.ServeMux, auth func(http.HandlerFunc) http.HandlerFunc, client *whatsmeow.Client) {
	mux.HandleFunc("/api/contacts/rename", auth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fail := func(status int, msg string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": msg})
		}

		var req ContactRenameRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.FullName) == "" {
			fail(http.StatusBadRequest, "phone and full_name are required")
			return
		}
		req.FullName = strings.TrimSpace(req.FullName)
		req.FirstName = strings.TrimSpace(req.FirstName)
		fmt.Printf("→ /api/contacts/rename phone=%q full_name=%q first_name=%q dry_run=%t from=%q\n",
			req.Phone, req.FullName, req.FirstName, req.DryRun, r.RemoteAddr)

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		pn, lid, err := resolveContactTarget(ctx, client.Store.LIDs, req.Phone)
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, errNoPhoneForLID) {
				status = http.StatusNotFound
			}
			fail(status, err.Error())
			return
		}
		before, err := client.Store.Contacts.GetContact(ctx, pn)
		if err != nil {
			fail(http.StatusInternalServerError, fmt.Sprintf("contact lookup failed: %v", err))
			return
		}
		patch := buildContactRename(pn, lid, req.FullName, req.FirstName)
		result := map[string]interface{}{
			"phone_jid":       pn.String(),
			"lid":             lid.String(),
			"before":          map[string]string{"full_name": before.FullName, "first_name": before.FirstName},
			"after":           map[string]string{"full_name": req.FullName, "first_name": req.FirstName},
			"dry_run":         req.DryRun,
			"index":           patch.Mutations[0].Index,
			"collection":      string(patch.Type),
			"contact_version": contactMutationVersion,
		}
		if req.DryRun {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": result})
			return
		}

		if !client.IsConnected() {
			fail(http.StatusServiceUnavailable, "Not connected to WhatsApp")
			return
		}
		if !labelWriteLimiter.allow(time.Now()) {
			fail(http.StatusTooManyRequests, fmt.Sprintf("write limit reached (%d per minute)", labelWriteLimit))
			return
		}
		if err := client.SendAppState(ctx, patch); err != nil {
			fail(http.StatusBadGateway, fmt.Sprintf("WhatsApp rejected the contact change: %v", err))
			return
		}
		// SendAppState fetches the collection back afterwards, which applies the
		// change to whatsmeow's contact store; read it to confirm.
		if stored, err := client.Store.Contacts.GetContact(ctx, pn); err == nil {
			result["stored"] = map[string]string{"full_name": stored.FullName, "first_name": stored.FirstName}
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "result": result})
	}))
}
