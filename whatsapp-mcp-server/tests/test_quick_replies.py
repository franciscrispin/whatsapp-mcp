"""Tests for the quick reply read tools (list_quick_replies, get_quick_reply).

The bridge writes quick replies into messages.db (whatsapp-bridge/quick_replies.go),
with keywords and linked label ids as JSON arrays.
"""

import json
import sqlite3

import pytest

import main as mcp_main
import whatsapp


@pytest.fixture
def qr_db(tmp_path, monkeypatch):
    path = tmp_path / "messages.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE labels (
            label_id TEXT PRIMARY KEY, name TEXT NOT NULL, color INTEGER NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMP NOT NULL, synced_at INTEGER NOT NULL
        );
        CREATE TABLE quick_replies (
            quick_reply_id TEXT PRIMARY KEY, shortcut TEXT NOT NULL, message TEXT NOT NULL,
            keywords TEXT NOT NULL, label_ids TEXT NOT NULL, use_count INTEGER NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMP NOT NULL, synced_at INTEGER NOT NULL
        );
        """
    )
    conn.execute("INSERT INTO labels VALUES ('7', 'TZ', 1, 0, '2026-10-01', 0)")
    rows = [
        ("1", "deposit", "Deposit is 30% of the trip price", ["payment"], ["7"], 12, 0),
        ("2", "Bhutan", "Bhutan visa needs a passport photo", [], [], 3, 0),
        ("3", "old", "Retired reply", [], [], 0, 1),
    ]
    conn.executemany(
        "INSERT INTO quick_replies VALUES (?, ?, ?, ?, ?, ?, ?, '2026-10-01', 0)",
        [(i, s, m, json.dumps(k), json.dumps(lbl), c, d) for i, s, m, k, lbl, c, d in rows],
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(path))
    return path


def test_list_quick_replies(qr_db):
    replies = mcp_main.list_quick_replies()
    assert [r["shortcut"] for r in replies] == ["Bhutan", "deposit"]
    dep = replies[1]
    assert dep["message"] == "Deposit is 30% of the trip price"
    assert dep["keywords"] == ["payment"]
    assert dep["labels"] == ["TZ"]
    assert dep["use_count"] == 12
    assert dep["deleted"] is False


def test_list_quick_replies_query_and_deleted(qr_db):
    assert [r["shortcut"] for r in mcp_main.list_quick_replies("PASSPORT")] == ["Bhutan"]
    assert [r["shortcut"] for r in mcp_main.list_quick_replies("/payment")] == ["deposit"]
    assert [r["shortcut"] for r in mcp_main.list_quick_replies(include_deleted=True)] == ["Bhutan", "deposit", "old"]


def test_get_quick_reply(qr_db):
    for shortcut in ("deposit", "/Deposit", " DEPOSIT "):
        out = mcp_main.get_quick_reply(shortcut)
        assert out["success"] is True
        assert out["quick_reply"]["quick_reply_id"] == "1"


def test_get_quick_reply_unknown_lists_known(qr_db):
    out = mcp_main.get_quick_reply("old")
    assert out["success"] is False
    assert out["message"] == "No such quick reply. Known shortcuts: /Bhutan, /deposit"
    assert mcp_main.get_quick_reply("")["success"] is False


def test_quick_reply_tools_before_bridge_has_synced(tmp_path, monkeypatch):
    path = tmp_path / "messages.db"
    sqlite3.connect(path).close()
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", str(path))
    assert mcp_main.list_quick_replies() == []
    assert mcp_main.get_quick_reply("deposit")["success"] is False
