# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for ``model_service.set_access_rules``.

The router hands over pydantic ``AccessRule`` instances from the request
body. The DB layer serialises with ``json.dumps`` which doesn't know how
to encode pydantic models — before the fix, PUT /model/{m}/access
returned 500 with a ``TypeError: Object of type AccessRule is not JSON
serializable`` deep in the vizgrams DB helper.

The fix normalises to plain dicts in the service layer before storing.
These tests pin the normalisation for the three shapes callers can send:
pydantic models (production API path), dicts (installer scripts /
programmatic callers), and duck-typed objects with .email / .role
attributes (defensive).
"""

from pathlib import Path
from unittest.mock import patch

from api.schemas.model import AccessRule
from api.services.model_service import set_access_rules


class _AttrRule:
    """A duck-typed rule — has .email and .role but is neither a dict
    nor a pydantic model. Guards against a future regression that
    tightens the type check too aggressively."""
    def __init__(self, email: str, role: str):
        self.email = email
        self.role = role


def _mock_setup(model_name: str = "oliverfenton"):
    """Patch load_registry so the service accepts the model and
    set_model_access_rules so we can inspect what actually reaches the
    DB layer without touching sqlite."""
    return patch.multiple(
        "api.services.model_service",
        load_registry=lambda md: {model_name: {}},
    )


def test_pydantic_access_rule_serialises_to_dict(tmp_path):
    """The production API path: FastAPI parses the JSON body into
    ``AccessRule`` pydantic instances and hands them to the service.
    Must convert to dict before hitting ``json.dumps`` in the DB layer."""
    rules_input = [AccessRule(email="you@example.com", role="ADMIN")]

    captured = {}
    with _mock_setup(), patch(
        "core.vizgrams_db.set_model_access_rules",
        side_effect=lambda name, r: captured.update({"name": name, "rules": r}),
    ):
        out = set_access_rules(Path("/nope"), "oliverfenton", rules_input)

    assert captured["name"] == "oliverfenton"
    assert captured["rules"] == [{"email": "you@example.com", "role": "ADMIN"}]
    # Return value uses the normalised dict form too so the router
    # response body reflects what's persisted.
    assert out == [{"email": "you@example.com", "role": "ADMIN"}]


def test_dict_input_passes_through_unchanged(tmp_path):
    """Programmatic callers (e.g. ops/scripts/install_model.py) send
    dicts directly. Must not double-serialise or wrap."""
    rules_input = [{"email": "you@example.com", "role": "ADMIN"}]

    captured = {}
    with _mock_setup(), patch(
        "core.vizgrams_db.set_model_access_rules",
        side_effect=lambda name, r: captured.update({"rules": r}),
    ):
        set_access_rules(Path("/nope"), "oliverfenton", rules_input)

    assert captured["rules"] == [{"email": "you@example.com", "role": "ADMIN"}]


def test_none_input_clears_rules(tmp_path):
    """None means "revert to config.yaml fallback" — must reach the DB
    layer as None, not as an empty list (which the DB layer treats as
    "open to no one — hide from everyone")."""
    captured = {}
    with _mock_setup(), patch(
        "core.vizgrams_db.set_model_access_rules",
        side_effect=lambda name, r: captured.update({"rules": r}),
    ):
        out = set_access_rules(Path("/nope"), "oliverfenton", None)

    assert captured["rules"] is None
    assert out is None


def test_attribute_style_input_normalises(tmp_path):
    """Defensive: a caller could hand in duck-typed objects. Normalise
    via .email / .role attributes."""
    rules_input = [_AttrRule(email="you@example.com", role="VIEWER")]

    captured = {}
    with _mock_setup(), patch(
        "core.vizgrams_db.set_model_access_rules",
        side_effect=lambda name, r: captured.update({"rules": r}),
    ):
        set_access_rules(Path("/nope"), "oliverfenton", rules_input)

    assert captured["rules"] == [{"email": "you@example.com", "role": "VIEWER"}]


def test_mixed_shapes_all_normalise(tmp_path):
    """One PUT could carry pydantic + dict + duck-typed rules if a
    caller mixes construction styles. All three must land as dicts."""
    rules_input = [
        AccessRule(email="a@example.com", role="ADMIN"),
        {"email": "b@example.com", "role": "OPERATOR"},
        _AttrRule(email="c@example.com", role="VIEWER"),
    ]

    captured = {}
    with _mock_setup(), patch(
        "core.vizgrams_db.set_model_access_rules",
        side_effect=lambda name, r: captured.update({"rules": r}),
    ):
        set_access_rules(Path("/nope"), "oliverfenton", rules_input)

    assert captured["rules"] == [
        {"email": "a@example.com", "role": "ADMIN"},
        {"email": "b@example.com", "role": "OPERATOR"},
        {"email": "c@example.com", "role": "VIEWER"},
    ]
