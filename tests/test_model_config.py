# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for core/model_config.py and validate_model_config."""

import os
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from core.model_config import load_database_config, load_model_config, resolve_credential, resolve_tool_config
from core.validation import validate_model_config

# --- resolve_credential ---

def test_resolve_literal():
    assert resolve_credential("mytoken123") == "mytoken123"


def test_resolve_literal_non_string_passthrough():
    assert resolve_credential(None) is None
    assert resolve_credential(42) == 42
    assert resolve_credential({"key": "val"}) == {"key": "val"}


def test_resolve_env_present():
    with patch.dict(os.environ, {"MY_SECRET": "abc123"}):
        assert resolve_credential("env:MY_SECRET") == "abc123"


def test_resolve_env_missing():
    """Unset env vars return None rather than raising — future-use fields may not be set."""
    os.environ.pop("NONEXISTENT_VAR_XYZ", None)
    assert resolve_credential("env:NONEXISTENT_VAR_XYZ") is None


def test_resolve_file_valid(tmp_path):
    secret_file = tmp_path / "token.txt"
    secret_file.write_text("  mysecret\n")
    result = resolve_credential(f"file:{secret_file}")
    assert result == "mysecret"


def test_resolve_file_missing():
    with pytest.raises(FileNotFoundError):
        resolve_credential("file:/nonexistent/path/to/token")


def test_resolve_file_tilde_expansion(tmp_path, monkeypatch):
    """~ in file: paths is expanded via Path.expanduser."""
    secret_file = tmp_path / "token.txt"
    secret_file.write_text("expanded_value")
    monkeypatch.setenv("HOME", str(tmp_path))
    result = resolve_credential("file:~/token.txt")
    assert result == "expanded_value"


def test_resolve_file_bare_name_with_secrets_dir(tmp_path, monkeypatch):
    """Bare name (no path separators) is resolved against VZ_SECRETS_DIR."""
    secret_file = tmp_path / "jira_token"
    secret_file.write_text("  baretoken\n")
    monkeypatch.setenv("VZ_SECRETS_DIR", str(tmp_path))
    result = resolve_credential("file:jira_token")
    assert result == "baretoken"


def test_resolve_file_bare_name_defaults_to_home_secrets(tmp_path, monkeypatch):
    """Bare name falls back to ~/.secrets/ when VZ_SECRETS_DIR is unset."""
    secrets_dir = tmp_path / ".secrets"
    secrets_dir.mkdir()
    secret_file = secrets_dir / "my_token"
    secret_file.write_text("hometoken")
    monkeypatch.delenv("VZ_SECRETS_DIR", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    result = resolve_credential("file:my_token")
    assert result == "hometoken"


# --- resolve_tool_config ---

def test_resolve_tool_config_resolves_api_token():
    with patch.dict(os.environ, {"MY_TOKEN": "resolved_token"}):
        raw = {
            "server": "https://example.com",
            "email": "user@example.com",
            "api_token": "env:MY_TOKEN",
            "field_aliases": {},
        }
        result = resolve_tool_config(raw)
    assert result["api_token"] == "resolved_token"
    assert result["server"] == "https://example.com"
    assert result["email"] == "user@example.com"
    assert result["field_aliases"] == {}


def test_resolve_tool_config_resolves_token():
    with patch.dict(os.environ, {"GH_TOKEN": "ghtoken"}):
        raw = {"org": "MyOrg", "token": "env:GH_TOKEN"}
        result = resolve_tool_config(raw)
    assert result["token"] == "ghtoken"
    assert result["org"] == "MyOrg"


def test_resolve_tool_config_non_credential_untouched():
    raw = {"enabled": True, "server": "https://x.com", "org": "Org"}
    result = resolve_tool_config(raw)
    assert result == raw


def test_resolve_tool_config_does_not_mutate_input():
    raw = {"api_token": "literal", "server": "s"}
    original = dict(raw)
    resolve_tool_config(raw)
    assert raw == original


# --- load_model_config ---

def test_load_model_config_absent(tmp_path):
    """Returns None when no config.yaml in model_dir."""
    assert load_model_config(tmp_path) is None


def test_load_model_config_present(tmp_path):
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text(textwrap.dedent("""\
        tools:
          git:
            enabled: true
            org: "MyOrg"
            host: "github.com"
          file:
            enabled: true
    """))
    result = load_model_config(tmp_path)
    assert result is not None
    assert result["git"]["org"] == "MyOrg"
    assert result["file"]["enabled"] is True


def test_load_model_config_returns_raw_credentials(tmp_path):
    """Credentials are NOT resolved at load time — still env:/file: strings."""
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text(textwrap.dedent("""\
        tools:
          jira:
            enabled: true
            server: "https://jira.example.com"
            email: "user@example.com"
            api_token: "env:JIRA_TOKEN"
    """))
    result = load_model_config(tmp_path)
    # Should be the raw string, not resolved
    assert result["jira"]["api_token"] == "env:JIRA_TOKEN"


def test_load_model_config_empty_tools(tmp_path):
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text("tools: {}\n")
    assert load_model_config(tmp_path) == {}


# --- validate_model_config ---


def _write_config(tmp_path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content))
    return p


def test_validate_valid_config(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: true
            server: "https://jira.example.com"
            email: "user@example.com"
            api_token: "env:JIRA_TOKEN"
            field_aliases: {}
          git:
            enabled: true
            org: "MyOrg"
            host: "github.com"
            token: "env:GH_TOKEN"
          git_codeowners:
            enabled: true
          file:
            enabled: true
    """)
    errors = validate_model_config(p)
    assert errors == []


def test_validate_missing_tools_key(tmp_path):
    p = _write_config(tmp_path, "other_key: value\n")
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.missing_tools_key" for e in errors)


def test_validate_jira_missing_server(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: true
            email: "user@example.com"
            api_token: "env:JIRA_TOKEN"
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.missing_required_field" and "server" in e.path for e in errors)


def test_validate_jira_missing_email(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: true
            server: "https://jira.example.com"
            api_token: "env:JIRA_TOKEN"
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.missing_required_field" and "email" in e.path for e in errors)


def test_validate_jira_missing_api_token(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: true
            server: "https://jira.example.com"
            email: "user@example.com"
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.missing_required_field" and "api_token" in e.path for e in errors)


def test_validate_git_missing_org(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          git:
            enabled: true
            host: "github.com"
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.missing_required_field" and "org" in e.path for e in errors)


def test_validate_disabled_tool_no_required_fields_needed(tmp_path):
    """Disabled tools are not checked for required fields."""
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: false
    """)
    errors = validate_model_config(p)
    assert not any(e.rule == "model_config.missing_required_field" for e in errors)


def test_validate_unknown_builtin_tool(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          database:
            enabled: true
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.unknown_tool" for e in errors)


def test_validate_custom_tool_valid(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          my_tool:
            enabled: true
            module: tools.my_tool.tool
            class: MyTool
    """)
    errors = validate_model_config(p)
    assert not any(e.rule == "model_config.unknown_tool" for e in errors)


def test_validate_custom_tool_module_without_class(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          my_tool:
            enabled: true
            module: tools.my_tool.tool
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.custom_tool_requires_class" for e in errors)


def test_validate_literal_credential_flagged(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: true
            server: "https://jira.example.com"
            email: "user@example.com"
            api_token: "plaintext_secret"
    """)
    errors = validate_model_config(p)
    assert any(e.rule == "model_config.literal_credential" for e in errors)


def test_validate_file_credential_not_flagged(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          jira:
            enabled: true
            server: "https://jira.example.com"
            email: "user@example.com"
            api_token: "file:jira_token"
    """)
    errors = validate_model_config(p)
    assert not any(e.rule == "model_config.literal_credential" for e in errors)


def test_validate_env_credential_not_flagged(tmp_path):
    p = _write_config(tmp_path, """\
        tools:
          git:
            enabled: true
            org: "MyOrg"
            token: "env:GH_TOKEN"
    """)
    errors = validate_model_config(p)
    assert not any(e.rule == "model_config.literal_credential" for e in errors)


# --- Tool constructor: config dict path ---

def test_github_tool_from_config_dict():
    from tools.git.tool import GitHubTool
    tool = GitHubTool(config={"org": "TestOrg", "host": "github.example.com"})
    assert tool.default_org == "TestOrg"
    assert tool.host == "github.example.com"


def test_github_tool_from_config_dict_missing_optional_fields():
    from tools.git.tool import GitHubTool
    tool = GitHubTool(config={"org": "TestOrg"})
    assert tool.default_org == "TestOrg"
    assert tool.host is None


def test_codeowners_tool_from_config_dict():
    from tools.git_codeowners.tool import CodeownersTool
    tool = CodeownersTool(config={"org": "TestOrg", "host": "github.com"})
    assert tool.default_org == "TestOrg"
    assert tool.host == "github.com"


# --- tool_service.get_tool_instance enforcement ---

def _write_tool_config(model_dir, tools: dict):
    import yaml
    (model_dir / "config.yaml").write_text(yaml.dump({"tools": tools}))


def test_get_tool_rejects_unlisted_tool(tmp_path):
    """When config.yaml is present, tools not listed must be rejected."""
    from api.services.tool_service import get_tool_instance

    _write_tool_config(tmp_path, {"git": {"enabled": True, "org": "X", "host": "github.com"}})
    with pytest.raises(ValueError, match="not listed"):
        get_tool_instance("jira", model_dir=tmp_path)


def test_get_tool_rejects_disabled_tool(tmp_path):
    """When config.yaml is present, disabled tools must be rejected."""
    from api.services.tool_service import get_tool_instance

    _write_tool_config(tmp_path, {"jira": {"enabled": False, "server": "s", "email": "e", "api_token": "t"}})
    with pytest.raises(ValueError, match="disabled"):
        get_tool_instance("jira", model_dir=tmp_path)


def test_get_tool_passes_model_dir_to_tool(tmp_path):
    """model_dir is forwarded to tool constructors."""
    from api.services.tool_service import BUILTIN_REGISTRY, get_tool_instance

    class CaptureTool:
        def __init__(self, config, model_dir=None, **_kw):
            self.received_config = config
            self.received_model_dir = model_dir
        def list_commands(self): return []

    BUILTIN_REGISTRY["capture_tool"] = CaptureTool
    try:
        _write_tool_config(tmp_path, {"capture_tool": {"enabled": True}})
        tool = get_tool_instance("capture_tool", model_dir=tmp_path)
        assert tool.received_model_dir == tmp_path
    finally:
        del BUILTIN_REGISTRY["capture_tool"]


# ---------------------------------------------------------------------------
# load_database_config
# ---------------------------------------------------------------------------

def _write_db_config(model_dir: Path, content: str) -> None:
    (model_dir / "config.yaml").write_text(textwrap.dedent(content))


class TestLoadDatabaseConfig:
    def test_no_config_returns_sqlite_default(self, tmp_path):
        """No config.yaml → SQLite backend (safe default for CI / local dev)."""
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "sqlite"
        assert "host" not in cfg
        assert "database" not in cfg

    def test_empty_config_returns_sqlite_default(self, tmp_path):
        (tmp_path / "config.yaml").write_text("{}\n")
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "sqlite"

    def test_config_without_database_key_returns_sqlite_default(self, tmp_path):
        _write_config(tmp_path, """\
            tools:
              github:
                enabled: true
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "sqlite"

    def test_sqlite_backend_explicit(self, tmp_path):
        _write_config(tmp_path, """\
            database:
              backend: sqlite
              path: data/custom.db
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "sqlite"
        assert cfg["path"] == "data/custom.db"

    def test_clickhouse_backend_full_config(self, tmp_path):
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: ch.internal
              port: 8123
              database: mymodel
              username: default
              password: ""
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "clickhouse"
        assert cfg["host"] == "ch.internal"
        assert cfg["port"] == 8123
        assert cfg["database"] == "mymodel"
        assert cfg["username"] == "default"

    def test_clickhouse_host_overrides_default(self, tmp_path):
        """Specifying only host overrides the env-var default; other keys use defaults."""
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: ch.internal
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "clickhouse"
        assert cfg["host"] == "ch.internal"
        assert cfg["username"] == "default"

    def test_partial_override_explicit_clickhouse_backend_merges_defaults(self, tmp_path):
        """Specifying backend: clickhouse with just host applies ClickHouse connection defaults."""
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: ch.internal
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "clickhouse"
        assert cfg["host"] == "ch.internal"
        assert cfg["username"] == "default"

    def test_returns_independent_copy(self, tmp_path):
        """Mutating the returned dict should not affect subsequent calls."""
        cfg1 = load_database_config(tmp_path)
        cfg1["backend"] = "mutated"
        cfg2 = load_database_config(tmp_path)
        assert cfg2["backend"] == "sqlite"

    def test_database_null_value_returns_sqlite_default(self, tmp_path):
        _write_config(tmp_path, "database:\n")
        cfg = load_database_config(tmp_path)
        assert cfg["backend"] == "sqlite"

    def test_env_var_resolved_for_host(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CH_HOST_TEST", "ch.internal")
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: env:CH_HOST_TEST
              port: 8123
              database: mydb
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["host"] == "ch.internal"

    def test_env_var_unset_falls_back_to_localhost(self, tmp_path, monkeypatch):
        """Unset CLICKHOUSE_HOST env var falls back to 'localhost' when backend is clickhouse."""
        monkeypatch.delenv("CLICKHOUSE_HOST", raising=False)
        _write_config(tmp_path, "database:\n  backend: clickhouse\n")
        cfg = load_database_config(tmp_path)
        assert cfg["host"] == "localhost"

    def test_non_string_values_pass_through_unchanged(self, tmp_path):
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: localhost
              port: 9000
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["port"] == 9000  # integer, not resolved

    def test_raw_sem_databases_derived_from_database(self, tmp_path):
        """raw_database is auto-derived as {database}_raw; sem_database equals the base database name."""
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: localhost
              port: 8123
              database: openflights
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["raw_database"] == "openflights_raw"
        assert cfg["sem_database"] == "openflights"

    def test_raw_sem_derived_from_model_dir_name(self, tmp_path):
        """When clickhouse backend set without a database key, raw/sem derived from dir name."""
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: localhost
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["raw_database"] == f"{tmp_path.name}_raw"
        assert cfg["sem_database"] == tmp_path.name

    def test_explicit_raw_sem_databases_override_derived(self, tmp_path):
        """Explicitly set raw_database / sem_database fields take precedence."""
        _write_config(tmp_path, """\
            database:
              backend: clickhouse
              host: localhost
              database: openflights
              raw_database: raw_custom
              sem_database: sem_custom
        """)
        cfg = load_database_config(tmp_path)
        assert cfg["raw_database"] == "raw_custom"
        assert cfg["sem_database"] == "sem_custom"

    def test_sqlite_backend_does_not_derive_raw_sem(self, tmp_path):
        """raw_database / sem_database are not added for SQLite backends."""
        _write_config(tmp_path, """\
            database:
              backend: sqlite
              path: data/data.db
        """)
        cfg = load_database_config(tmp_path)
        assert "raw_database" not in cfg
        assert "sem_database" not in cfg


# ---------------------------------------------------------------------------
# DB-first loading (VG-141)
# ---------------------------------------------------------------------------


class TestLoadModelConfigDbFirst:
    """load_model_config reads from DB when available, falls back to config.yaml."""

    def test_db_config_takes_priority_over_yaml(self, tmp_path, monkeypatch):
        # Write a config.yaml that would be used as fallback
        _write_config(tmp_path, """\
            tools:
              file:
                enabled: true
        """)
        # Monkeypatch DB to return a different config
        db_tools = {"jira": {"enabled": True, "server": "https://jira.example.com"}}
        monkeypatch.setattr(
            "core.vizgrams_db.load_model_config_from_db",
            lambda model_id, db_path=None: db_tools,
        )
        result = load_model_config(tmp_path)
        assert "jira" in result
        assert "file" not in result

    def test_falls_back_to_yaml_when_db_returns_none(self, tmp_path, monkeypatch):
        _write_config(tmp_path, """\
            tools:
              git:
                enabled: true
                org: FallbackOrg
        """)
        monkeypatch.setattr(
            "core.vizgrams_db.load_model_config_from_db",
            lambda model_id, db_path=None: None,
        )
        result = load_model_config(tmp_path)
        assert result["git"]["org"] == "FallbackOrg"

    def test_returns_none_when_db_empty_and_no_yaml(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "core.vizgrams_db.load_model_config_from_db",
            lambda model_id, db_path=None: None,
        )
        assert load_model_config(tmp_path) is None


class TestLoadDatabaseConfigDbFirst:
    """load_database_config reads from DB when available, falls back to config.yaml."""

    def test_db_config_takes_priority_over_yaml(self, tmp_path, monkeypatch):
        _write_config(tmp_path, """\
            database:
              backend: sqlite
        """)
        db_cfg = {"backend": "clickhouse", "host": "ch.prod", "database": "mymodel"}
        monkeypatch.setattr(
            "core.vizgrams_db.load_database_config_from_db",
            lambda model_id, db_path=None: db_cfg,
        )
        result = load_database_config(tmp_path)
        assert result["backend"] == "clickhouse"
        assert result["host"] == "ch.prod"

    def test_falls_back_to_yaml_when_db_returns_none(self, tmp_path, monkeypatch):
        _write_config(tmp_path, """\
            database:
              backend: duckdb
              path: data/data.duckdb
        """)
        monkeypatch.setattr(
            "core.vizgrams_db.load_database_config_from_db",
            lambda model_id, db_path=None: None,
        )
        result = load_database_config(tmp_path)
        assert result["backend"] == "duckdb"

    def test_defaults_applied_to_db_config(self, tmp_path, monkeypatch):
        """DB config still gets defaults + credential resolution applied."""
        monkeypatch.setattr(
            "core.vizgrams_db.load_database_config_from_db",
            lambda model_id, db_path=None: {"backend": "clickhouse", "host": "ch.prod"},
        )
        monkeypatch.delenv("CLICKHOUSE_HOST", raising=False)
        result = load_database_config(tmp_path)
        assert result["backend"] == "clickhouse"
        assert result["host"] == "ch.prod"
        assert result["username"] == "default"  # from _CLICKHOUSE_DEFAULTS
        assert result["database"] == tmp_path.name  # derived from dir name

    def test_sqlite_default_when_db_empty_and_no_yaml(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "core.vizgrams_db.load_database_config_from_db",
            lambda model_id, db_path=None: None,
        )
        result = load_database_config(tmp_path)
        assert result["backend"] == "sqlite"
