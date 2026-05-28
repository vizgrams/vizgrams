# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/rbac.py — role resolution and system admin checks."""

import yaml

from core.rbac import ModelRole, _matches, get_model_role, is_member, is_system_admin

# ---------------------------------------------------------------------------
# _matches
# ---------------------------------------------------------------------------

class TestMatches:
    def test_exact_match(self):
        assert _matches("user@example.com", "user@example.com")

    def test_exact_case_insensitive(self):
        assert _matches("User@Example.COM", "user@example.com")

    def test_exact_no_match(self):
        assert not _matches("other@example.com", "user@example.com")

    def test_domain_wildcard_match(self):
        assert _matches("any@example.com", "*@example.com")

    def test_domain_wildcard_no_match(self):
        assert not _matches("user@other.com", "*@example.com")

    def test_domain_wildcard_case_insensitive(self):
        assert _matches("USER@EXAMPLE.COM", "*@example.com")

    def test_catch_all(self):
        assert _matches("anyone@anywhere.com", "*")

    def test_no_partial_domain_match(self):
        # *@example.com should not match notexample.com
        assert not _matches("user@notexample.com", "*@example.com")


# ---------------------------------------------------------------------------
# is_system_admin
# ---------------------------------------------------------------------------

class TestIsSystemAdmin:
    def test_dev_user_is_system_admin(self, monkeypatch):
        monkeypatch.setenv("DEV_USER", "dev@local.com")
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        assert is_system_admin("dev@local.com")

    def test_dev_user_other_email_not_admin(self, monkeypatch):
        monkeypatch.setenv("DEV_USER", "dev@local.com")
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        assert not is_system_admin("other@local.com")

    def test_exact_email_in_system_admins(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@example.com")
        assert is_system_admin("admin@example.com")

    def test_domain_wildcard_in_system_admins(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.setenv("VZ_SYSTEM_ADMINS", "*@example.com")
        assert is_system_admin("anyone@example.com")

    def test_multiple_patterns(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.setenv("VZ_SYSTEM_ADMINS", "boss@corp.com,*@admin.example.com")
        assert is_system_admin("boss@corp.com")
        assert is_system_admin("it@admin.example.com")
        assert not is_system_admin("user@corp.com")

    def test_not_admin_when_unset(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        assert not is_system_admin("user@example.com")


# ---------------------------------------------------------------------------
# get_model_role
# ---------------------------------------------------------------------------

def _write_config(model_dir, access=None, tools=None):
    data = {}
    if access is not None:
        data["access"] = access
    if tools is not None:
        data["tools"] = tools
    (model_dir / "config.yaml").write_text(yaml.dump(data))


class TestGetModelRole:
    def test_system_admin_always_gets_admin(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@example.com")
        monkeypatch.delenv("DEV_USER", raising=False)
        _write_config(tmp_path, access=[
            {"email": "admin@example.com", "role": "VIEWER"},
        ])
        # Even though the model config says VIEWER, system admin wins
        assert get_model_role(tmp_path, "admin@example.com") == ModelRole.ADMIN

    def test_no_config_file_returns_admin(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        assert get_model_role(tmp_path, "user@example.com") == ModelRole.ADMIN

    def test_config_no_access_block_returns_admin(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, tools={"git": {"enabled": True}})
        assert get_model_role(tmp_path, "user@example.com") == ModelRole.ADMIN

    def test_exact_email_match(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, access=[
            {"email": "alice@example.com", "role": "OPERATOR"},
        ])
        assert get_model_role(tmp_path, "alice@example.com") == ModelRole.OPERATOR

    def test_domain_wildcard_match(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, access=[
            {"email": "*@example.com", "role": "VIEWER"},
        ])
        assert get_model_role(tmp_path, "anyone@example.com") == ModelRole.VIEWER

    def test_catch_all_entry(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, access=[
            {"email": "*@example.com", "role": "OPERATOR"},
            {"email": "*", "role": "VIEWER"},
        ])
        assert get_model_role(tmp_path, "other@elsewhere.com") == ModelRole.VIEWER

    def test_first_match_wins(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, access=[
            {"email": "alice@example.com", "role": "ADMIN"},
            {"email": "*@example.com", "role": "VIEWER"},
        ])
        # alice matches first entry (ADMIN), not second (*@example.com → VIEWER)
        assert get_model_role(tmp_path, "alice@example.com") == ModelRole.ADMIN

    def test_no_match_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, access=[
            {"email": "alice@example.com", "role": "ADMIN"},
        ])
        assert get_model_role(tmp_path, "outsider@other.com") is None

    def test_all_three_roles_resolved(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        for role_str, expected in [
            ("VIEWER", ModelRole.VIEWER),
            ("OPERATOR", ModelRole.OPERATOR),
            ("ADMIN", ModelRole.ADMIN),
        ]:
            _write_config(tmp_path, access=[{"email": "u@x.com", "role": role_str}])
            assert get_model_role(tmp_path, "u@x.com") == expected

    def test_role_hierarchy(self):
        assert ModelRole.VIEWER < ModelRole.OPERATOR < ModelRole.ADMIN

    # --- DB access rules take priority over config.yaml ---

    def test_db_rules_override_config_yaml(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        # config.yaml says ADMIN for everyone
        _write_config(tmp_path, access=[{"email": "*", "role": "ADMIN"}])
        # DB says VIEWER for this user
        monkeypatch.setattr(
            "core.vizgrams_db.get_model_access_rules",
            lambda model_id, db_path=None: [{"email": "alice@example.com", "role": "VIEWER"}],
        )
        assert get_model_role(tmp_path, "alice@example.com") == ModelRole.VIEWER

    def test_db_empty_rules_means_open_admin(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        # config.yaml restricts to VIEWER
        _write_config(tmp_path, access=[{"email": "*", "role": "VIEWER"}])
        # DB has an empty list (explicitly open)
        monkeypatch.setattr(
            "core.vizgrams_db.get_model_access_rules",
            lambda model_id, db_path=None: [],
        )
        assert get_model_role(tmp_path, "alice@example.com") == ModelRole.ADMIN

    def test_db_rules_none_falls_back_to_config(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        _write_config(tmp_path, access=[{"email": "alice@example.com", "role": "OPERATOR"}])
        # DB returns None → fall back to config.yaml
        monkeypatch.setattr(
            "core.vizgrams_db.get_model_access_rules",
            lambda model_id, db_path=None: None,
        )
        assert get_model_role(tmp_path, "alice@example.com") == ModelRole.OPERATOR

    def test_db_rules_no_match_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.setattr(
            "core.vizgrams_db.get_model_access_rules",
            lambda model_id, db_path=None: [{"email": "alice@example.com", "role": "ADMIN"}],
        )
        assert get_model_role(tmp_path, "outsider@other.com") is None

    def test_db_domain_wildcard_in_rules(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.setattr(
            "core.vizgrams_db.get_model_access_rules",
            lambda model_id, db_path=None: [{"email": "*@acme.com", "role": "OPERATOR"}],
        )
        assert get_model_role(tmp_path, "bob@acme.com") == ModelRole.OPERATOR
        assert get_model_role(tmp_path, "bob@other.com") is None

    def test_system_admin_bypasses_db_rules(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@example.com")
        monkeypatch.delenv("DEV_USER", raising=False)
        # DB says VIEWER
        monkeypatch.setattr(
            "core.vizgrams_db.get_model_access_rules",
            lambda model_id, db_path=None: [{"email": "*", "role": "VIEWER"}],
        )
        assert get_model_role(tmp_path, "admin@example.com") == ModelRole.ADMIN


# ---------------------------------------------------------------------------
# is_member (Epic 26 VG-292 — replaces is_creator)
#
# Any authenticated user is a member. The old VZ_CREATORS env var is
# ignored — membership is no longer gated by an explicit allowlist.
# ---------------------------------------------------------------------------

class TestIsMember:
    def test_authenticated_user_is_member(self):
        assert is_member("alice@example.com") is True

    def test_unauthenticated_request_is_not_member(self):
        assert is_member(None) is False

    def test_empty_string_is_not_member(self):
        """Empty string is treated like no email — defensive against
        upstream auth setting a blank header instead of omitting it."""
        assert is_member("") is False

    def test_vz_creators_env_no_longer_gates_membership(self, monkeypatch):
        """Setting VZ_CREATORS used to grant creator role. After the
        collapse it has no effect — any signed-in user is a member."""
        monkeypatch.setenv("VZ_CREATORS", "alice@example.com")
        # Both users are members because both are authenticated.
        assert is_member("alice@example.com") is True
        assert is_member("bob@example.com") is True
