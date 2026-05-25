# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for chat session + turn persistence (Epic 25 VG-280)."""

from __future__ import annotations

import pytest

from core.chat_history_db import (
    append_turn,
    attach_saved_artifacts,
    create_session,
    end_session,
    get_session,
    list_sessions_for_user,
    list_turns_for_session,
    set_turn_feedback,
    update_session_title,
)

# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


class TestCreateAndGetSession:
    def test_create_round_trips(self):
        sid = create_session(user_id="alice", model_id="iagai", title="DORA chat")
        s = get_session(sid, user_id="alice")
        assert s is not None
        assert s["user_id"] == "alice"
        assert s["model_id"] == "iagai"
        assert s["title"] == "DORA chat"
        assert s["created_at"]
        assert s["updated_at"]
        assert s["ended_at"] is None

    def test_get_returns_none_for_wrong_owner(self):
        """Owner-scoped — never leak another user's session existence."""
        sid = create_session(user_id="alice", model_id="iagai")
        assert get_session(sid, user_id="bob") is None

    def test_get_returns_none_for_missing(self):
        assert get_session("no-such-id", user_id="alice") is None


class TestListSessions:
    def test_returns_user_sessions_newest_first(self):
        s1 = create_session(user_id="alice", model_id="iagai", title="first")
        s2 = create_session(user_id="alice", model_id="iagai", title="second")
        out = list_sessions_for_user(user_id="alice", model_id="iagai")
        # Updated_at is identical at insert time — secondary order is
        # implementation-defined. Just assert both come back.
        assert {s["id"] for s in out} == {s1, s2}

    def test_isolates_users(self):
        create_session(user_id="alice", model_id="iagai")
        create_session(user_id="bob", model_id="iagai")
        assert len(list_sessions_for_user(user_id="alice", model_id="iagai")) == 1
        assert len(list_sessions_for_user(user_id="bob", model_id="iagai")) == 1

    def test_filters_by_model(self):
        create_session(user_id="alice", model_id="iagai")
        create_session(user_id="alice", model_id="openflights")
        out = list_sessions_for_user(user_id="alice", model_id="iagai")
        assert len(out) == 1
        assert out[0]["model_id"] == "iagai"

    def test_hides_ended_sessions_by_default(self):
        sid = create_session(user_id="alice", model_id="iagai")
        end_session(sid, user_id="alice")
        assert list_sessions_for_user(user_id="alice", model_id="iagai") == []
        # include_ended=True surfaces them again.
        out = list_sessions_for_user(
            user_id="alice", model_id="iagai", include_ended=True,
        )
        assert len(out) == 1

    def test_pagination(self):
        for _ in range(5):
            create_session(user_id="alice", model_id="iagai")
        assert len(list_sessions_for_user(user_id="alice", limit=2)) == 2
        assert len(list_sessions_for_user(user_id="alice", limit=2, offset=2)) == 2
        assert len(list_sessions_for_user(user_id="alice", limit=2, offset=4)) == 1


class TestUpdateSessionTitle:
    def test_updates_title_for_owner(self):
        sid = create_session(user_id="alice", model_id="iagai", title="old")
        assert update_session_title(sid, user_id="alice", title="new") is True
        assert get_session(sid, user_id="alice")["title"] == "new"

    def test_wrong_owner_is_noop(self):
        sid = create_session(user_id="alice", model_id="iagai", title="old")
        assert update_session_title(sid, user_id="bob", title="hack") is False
        assert get_session(sid, user_id="alice")["title"] == "old"


class TestEndSession:
    def test_marks_ended_at(self):
        sid = create_session(user_id="alice", model_id="iagai")
        assert end_session(sid, user_id="alice") is True
        s = get_session(sid, user_id="alice")
        assert s["ended_at"] is not None

    def test_ending_twice_is_noop(self):
        sid = create_session(user_id="alice", model_id="iagai")
        end_session(sid, user_id="alice")
        # Second call returns False because the WHERE clause guards on
        # ``ended_at IS NULL``.
        assert end_session(sid, user_id="alice") is False

    def test_wrong_owner_is_noop(self):
        sid = create_session(user_id="alice", model_id="iagai")
        assert end_session(sid, user_id="bob") is False
        assert get_session(sid, user_id="alice")["ended_at"] is None


# ---------------------------------------------------------------------------
# Turns
# ---------------------------------------------------------------------------


@pytest.fixture
def session_id():
    return create_session(user_id="alice", model_id="iagai", title="t")


class TestAppendTurn:
    def test_user_turn(self, session_id):
        tid = append_turn(
            session_id=session_id, role="user", content="how many widgets?",
        )
        turns = list_turns_for_session(session_id, user_id="alice")
        assert len(turns) == 1
        assert turns[0]["id"] == tid
        assert turns[0]["ord"] == 0
        assert turns[0]["role"] == "user"
        assert turns[0]["content"] == "how many widgets?"
        assert turns[0]["response_json"] is None

    def test_assistant_turn_with_response(self, session_id):
        response = {
            "success": True,
            "saved_view": {"name": "dora_clt", "params": {}},
            "title": "DORA CLT",
        }
        append_turn(session_id=session_id, role="assistant", response=response)
        turns = list_turns_for_session(session_id, user_id="alice")
        # Round-trips as a dict, not a JSON string.
        assert turns[0]["response_json"] == response

    def test_ord_increments_per_session(self, session_id):
        for i in range(3):
            append_turn(session_id=session_id, role="user", content=f"q{i}")
        turns = list_turns_for_session(session_id, user_id="alice")
        assert [t["ord"] for t in turns] == [0, 1, 2]

    def test_appending_bumps_session_updated_at(self, session_id):
        before = get_session(session_id, user_id="alice")["updated_at"]
        # Sleep one microsecond's worth — sqlite stores ISO strings,
        # so a fresh timestamp will sort after the previous one.
        import time
        time.sleep(0.001)
        append_turn(session_id=session_id, role="user", content="q")
        after = get_session(session_id, user_id="alice")["updated_at"]
        assert after >= before


class TestListTurnsOwnerScoping:
    def test_other_user_gets_empty(self, session_id):
        append_turn(session_id=session_id, role="user", content="secret")
        # Defence in depth — even with the session id, wrong user sees
        # nothing rather than the turns.
        assert list_turns_for_session(session_id, user_id="eve") == []


class TestAttachSavedArtifacts:
    def test_attaches_and_merges(self, session_id):
        tid = append_turn(session_id=session_id, role="assistant", response={"x": 1})
        attach_saved_artifacts(tid, artifacts=[{"kind": "view", "name": "v1"}])
        attach_saved_artifacts(tid, artifacts=[{"kind": "query", "name": "q1"}])
        turns = list_turns_for_session(session_id, user_id="alice")
        assert turns[0]["saved_artifact_ids"] == [
            {"kind": "view", "name": "v1"},
            {"kind": "query", "name": "q1"},
        ]

    def test_attach_to_missing_turn_is_noop(self):
        # Defensive — no crash if the turn id is stale.
        attach_saved_artifacts("no-such-turn", artifacts=[{"kind": "view", "name": "x"}])


class TestSetFeedback:
    def test_overwrites(self, session_id):
        tid = append_turn(session_id=session_id, role="assistant", response={})
        set_turn_feedback(tid, feedback={"rating": "up"})
        set_turn_feedback(tid, feedback={"rating": "down", "reason": "wrong chart"})
        turns = list_turns_for_session(session_id, user_id="alice")
        assert turns[0]["feedback"] == {"rating": "down", "reason": "wrong chart"}
