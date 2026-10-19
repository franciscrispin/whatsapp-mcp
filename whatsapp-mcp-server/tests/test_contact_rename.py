"""Tests for the rename_contact tool, which calls the bridge's /api/contacts/rename."""

import pytest

import main as mcp_main
import whatsapp

PHONE = "6591234567"


class _BridgeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


@pytest.fixture
def bridge_calls(monkeypatch):
    calls = []
    reply = {
        "status": 200,
        "body": {
            "success": True,
            "result": {
                "phone_jid": f"{PHONE}@s.whatsapp.net",
                "before": {"full_name": "Ann", "first_name": ""},
                "after": {"full_name": "Ann Tan", "first_name": "Ann"},
            },
        },
    }

    def fake_post(url, json, headers=None, timeout=None):
        calls.append((url, json))
        return _BridgeResponse(reply["status"], reply["body"])

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)
    return calls, reply


def test_rename_contact_calls_bridge(bridge_calls):
    calls, _ = bridge_calls
    out = mcp_main.rename_contact(PHONE, "Ann Tan", "Ann")
    assert calls == [
        (
            f"{whatsapp.WHATSAPP_API_BASE_URL}/contacts/rename",
            {"phone": PHONE, "full_name": "Ann Tan", "first_name": "Ann", "dry_run": False},
        )
    ]
    assert out["success"] is True
    assert out["message"] == "Renamed Ann to Ann Tan"
    assert out["before"] == {"full_name": "Ann", "first_name": ""}
    assert out["changed"] is True


def test_rename_contact_dry_run(bridge_calls):
    calls, _ = bridge_calls
    out = mcp_main.rename_contact(PHONE, "Ann Tan", dry_run=True)
    assert calls[0][1]["dry_run"] is True
    assert out["message"].startswith("Dry run:")
    assert out["changed"] is False


def test_rename_contact_requires_name(bridge_calls):
    calls, _ = bridge_calls
    assert mcp_main.rename_contact(PHONE, "  ")["success"] is False
    assert mcp_main.rename_contact("", "Ann")["success"] is False
    assert calls == []


def test_rename_contact_surfaces_bridge_error(bridge_calls):
    _, reply = bridge_calls
    reply["status"] = 502
    reply["body"] = {"success": False, "message": "WhatsApp rejected the contact change: boom"}
    out = mcp_main.rename_contact(PHONE, "Ann Tan")
    assert out == {"success": False, "message": "WhatsApp rejected the contact change: boom"}
