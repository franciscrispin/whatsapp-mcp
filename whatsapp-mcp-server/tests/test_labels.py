"""Tests for the label read tools (list_labels, get_chat_labels, get_label_chats).

The bridge writes labels into messages.db (whatsapp-bridge/labels.go). One-to-one
associations usually arrive keyed by LID, so the read side has to resolve them
through whatsmeow_lid_map and still return something useful when it cannot.
"""

import sqlite3

import pytest

import main as mcp_main
import whatsapp

PHONE = "6591234567"
LID = "111111111111111"
UNMAPPED_LID = "222222222222222"
GROUP = "120363000000000001@g.us"


@pytest.fixture
def label_dbs(tmp_path, monkeypatch):
    msg_path = tmp_path / "messages.db"
    wa_path = tmp_path / "whatsapp.db"

    conn = sqlite3.connect(msg_path)
    conn.executescript(
        """
        CREATE TABLE chats (jid TEXT PRIMARY KEY, name TEXT, last_message_time TIMESTAMP);
        CREATE TABLE labels (
            label_id TEXT PRIMARY KEY, name TEXT NOT NULL, color INTEGER NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMP NOT NULL, synced_at INTEGER NOT NULL
        );
        CREATE TABLE label_chats (
            label_id TEXT NOT NULL, chat_jid TEXT NOT NULL, labeled INTEGER NOT NULL,
            updated_at TIMESTAMP NOT NULL, synced_at INTEGER NOT NULL, PRIMARY KEY (label_id, chat_jid)
        );
        """
    )
    conn.executemany(
        "INSERT INTO labels VALUES (?, ?, ?, ?, '2026-10-13', 0)",
        [
            ("7", "TZ", 18, 0),
            ("15", "Won - Status", 10, 0),
            ("25", "‎Favorites", 0, 0),
            ("6", "Old label", 2, 1),
        ],
    )
    conn.executemany(
        "INSERT INTO label_chats VALUES (?, ?, ?, '2026-10-13', 0)",
        [
            ("7", f"{LID}@lid", 1),
            ("7", f"{UNMAPPED_LID}@lid", 1),
            ("7", GROUP, 1),
            ("15", f"{LID}@lid", 1),
            ("15", f"{UNMAPPED_LID}@lid", 0),  # removed: must not show
            ("6", f"{LID}@lid", 1),  # on a deleted label: must not show
        ],
    )
    conn.executemany(
        "INSERT INTO chats VALUES (?, ?, NULL)",
        [(f"{PHONE}@s.whatsapp.net", "Jane Tan (GE6 - Group)"), (GROUP, "TZ50 trip group")],
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(wa_path)
    conn.executescript(
        """
        CREATE TABLE whatsmeow_lid_map (lid TEXT PRIMARY KEY, pn TEXT);
        CREATE TABLE whatsmeow_contacts (
            our_jid TEXT, their_jid TEXT, first_name TEXT, full_name TEXT, push_name TEXT, business_name TEXT
        );
        """
    )
    conn.execute("INSERT INTO whatsmeow_lid_map VALUES (?, ?)", (LID, PHONE))
    conn.execute(
        "INSERT INTO whatsmeow_contacts VALUES ('me', ?, NULL, NULL, 'Bob', NULL)",
        (f"{UNMAPPED_LID}@lid",),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(msg_path))
    monkeypatch.setattr(whatsapp, "WHATSMEOW_DB_PATH", str(wa_path))
    return msg_path


def test_list_labels_counts_live_associations_and_hides_deleted(label_dbs):
    labels = mcp_main.list_labels()
    by_id = {lbl["label_id"]: lbl for lbl in labels}
    assert set(by_id) == {"7", "15", "25"}
    assert by_id["7"]["chat_count"] == 3
    assert by_id["15"]["chat_count"] == 1
    # Preset names lose WhatsApp's invisible marker.
    assert by_id["25"]["name"] == "Favorites"
    # Sorted numerically, not as strings.
    assert [lbl["label_id"] for lbl in labels] == ["7", "15", "25"]


def test_list_labels_can_include_deleted(label_dbs):
    labels = mcp_main.list_labels(include_deleted=True)
    old = next(lbl for lbl in labels if lbl["label_id"] == "6")
    assert old["deleted"] is True


@pytest.mark.parametrize("identifier", [PHONE, f"{PHONE}@s.whatsapp.net", LID, f"{LID}@lid"])
def test_get_chat_labels_accepts_phone_or_lid(label_dbs, identifier):
    names = [lbl["name"] for lbl in mcp_main.get_chat_labels(identifier)]
    assert names == ["TZ", "Won - Status"]


def test_get_chat_labels_for_group(label_dbs):
    assert [lbl["name"] for lbl in mcp_main.get_chat_labels(GROUP)] == ["TZ"]


def test_get_label_chats_resolves_lids_and_flags_gaps(label_dbs):
    result = mcp_main.get_label_chats(label_name="tz")
    assert result["success"] is True
    assert result["label"]["label_id"] == "7"
    assert result["total"] == 3
    assert result["without_phone_number"] == 1

    chats = {c["label_jid"]: c for c in result["chats"]}
    mapped = chats[f"{LID}@lid"]
    assert mapped["jid"] == f"{PHONE}@s.whatsapp.net"
    assert mapped["phone_number"] == PHONE
    assert mapped["name"] == "Jane Tan (GE6 - Group)"
    assert mapped["in_archive"] is True

    unmapped = chats[f"{UNMAPPED_LID}@lid"]
    assert unmapped["jid"] == f"{UNMAPPED_LID}@lid"
    assert unmapped["phone_number"] is None
    assert unmapped["name"] == "Bob"
    assert unmapped["in_archive"] is False

    assert chats[GROUP]["is_group"] is True
    assert chats[GROUP]["name"] == "TZ50 trip group"


def test_get_label_chats_excludes_removed_associations(label_dbs):
    result = mcp_main.get_label_chats(label_id="15")
    assert [c["label_jid"] for c in result["chats"]] == [f"{LID}@lid"]


def test_get_label_chats_unknown_name_lists_known_labels(label_dbs):
    result = mcp_main.get_label_chats(label_name="Zanzibar")
    assert result["success"] is False
    assert "TZ" in result["message"]


def test_get_label_chats_needs_an_identifier(label_dbs):
    assert mcp_main.get_label_chats()["success"] is False


def test_label_tools_before_bridge_has_synced(tmp_path, monkeypatch):
    path = tmp_path / "messages.db"
    sqlite3.connect(path).close()
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(path))
    assert mcp_main.list_labels() == []
    assert mcp_main.get_chat_labels(PHONE) == []
    assert mcp_main.get_label_chats(label_name="TZ")["success"] is False
