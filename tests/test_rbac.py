# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/rbac.py — role resolution and system admin checks."""

import yaml

from core.rbac import ModelRole, _matches, get_model_role, is_creator, is_system_admin

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


# ---------------------------------------------------------------------------
# is_creator
# ---------------------------------------------------------------------------

class TestIsCreator:
    def test_system_admin_is_creator(self, monkeypatch):
        monkeypatch.setenv("VZ_SYSTEM_ADMINS", "admin@example.com")
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_CREATORS", raising=False)
        assert is_creator("admin@example.com")

    def test_dev_user_is_creator(self, monkeypatch):
        monkeypatch.setenv("DEV_USER", "dev@local.com")
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.delenv("VZ_CREATORS", raising=False)
        assert is_creator("dev@local.com")

    def test_explicit_creator_email(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.setenv("VZ_CREATORS", "alice@example.com")
        assert is_creator("alice@example.com")
        assert not is_creator("bob@example.com")

    def test_creator_domain_wildcard(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.setenv("VZ_CREATORS", "*@startup.io")
        assert is_creator("anyone@startup.io")
        assert not is_creator("user@other.com")

    def test_multiple_creator_patterns(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.setenv("VZ_CREATORS", "alice@x.com,*@startup.io")
        assert is_creator("alice@x.com")
        assert is_creator("bob@startup.io")
        assert not is_creator("eve@other.com")

    def test_not_creator_when_unset(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.delenv("VZ_CREATORS", raising=False)
        assert not is_creator("user@example.com")

    def test_creator_catch_all(self, monkeypatch):
        monkeypatch.delenv("DEV_USER", raising=False)
        monkeypatch.delenv("VZ_SYSTEM_ADMINS", raising=False)
        monkeypatch.setenv("VZ_CREATORS", "*")
        assert is_creator("anyone@anywhere.com")
