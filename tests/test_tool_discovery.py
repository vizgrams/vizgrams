# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for system tool discovery and registration (VG-150)."""

import textwrap

from core.tool_service import (
    BUILTIN_REGISTRY,
    SYSTEM_REGISTRY,
    _resolve_class,
    discover_system_tools,
    list_available_tools,
)
from tools.base import BaseTool


def _write_tool(tools_dir, name, code):
    """Write a tool.py into tools_dir/name/tool.py."""
    tool_dir = tools_dir / name
    tool_dir.mkdir(parents=True)
    (tool_dir / "tool.py").write_text(textwrap.dedent(code))


_SIMPLE_TOOL = """\
    from tools.base import BaseTool

    class DemoTool(BaseTool):
        PARAMS = {
            "api_key": {"required": True, "description": "API key", "credential": True},
        }

        def run(self, command, params=None):
            yield {"ok": True}

        def list_commands(self):
            return ["fetch"]
"""

_NO_PARAMS_TOOL = """\
    from tools.base import BaseTool

    class SimpleTool(BaseTool):
        def run(self, command, params=None):
            yield {}

        def list_commands(self):
            return ["load"]
"""


# ---------------------------------------------------------------------------
# discover_system_tools
# ---------------------------------------------------------------------------

class TestDiscoverSystemTools:
    def test_empty_dir(self, tmp_path):
        result = discover_system_tools([tmp_path])
        assert result == {}

    def test_nonexistent_dir(self, tmp_path):
        result = discover_system_tools([tmp_path / "nope"])
        assert result == {}

    def test_discovers_valid_tool(self, tmp_path):
        _write_tool(tmp_path, "demo", _SIMPLE_TOOL)
        result = discover_system_tools([tmp_path])
        assert "demo" in result
        assert issubclass(result["demo"], BaseTool)

    def test_discovers_tool_without_params(self, tmp_path):
        _write_tool(tmp_path, "simple", _NO_PARAMS_TOOL)
        result = discover_system_tools([tmp_path])
        assert "simple" in result
        assert result["simple"].PARAMS == {}

    def test_skips_dir_without_tool_py(self, tmp_path):
        (tmp_path / "empty_tool").mkdir()
        result = discover_system_tools([tmp_path])
        assert result == {}

    def test_skips_builtin_collision(self, tmp_path):
        # "jira" is a built-in — should be skipped
        _write_tool(tmp_path, "jira", _SIMPLE_TOOL)
        result = discover_system_tools([tmp_path])
        assert "jira" not in result

    def test_skips_no_base_tool_subclass(self, tmp_path):
        tool_dir = tmp_path / "bad_tool"
        tool_dir.mkdir()
        (tool_dir / "tool.py").write_text("class NotATool:\n    pass\n")
        result = discover_system_tools([tmp_path])
        assert "bad_tool" not in result

    def test_multiple_dirs_first_wins(self, tmp_path):
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        _write_tool(dir1, "demo", _SIMPLE_TOOL)
        _write_tool(dir2, "demo", _NO_PARAMS_TOOL)
        result = discover_system_tools([dir1, dir2])
        assert "demo" in result
        # First dir wins — should have PARAMS with api_key
        assert "api_key" in result["demo"].PARAMS

    def test_multiple_dirs_merge(self, tmp_path):
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        _write_tool(dir1, "tool_a", _SIMPLE_TOOL)
        _write_tool(dir2, "tool_b", _NO_PARAMS_TOOL)
        result = discover_system_tools([dir1, dir2])
        assert "tool_a" in result
        assert "tool_b" in result


# ---------------------------------------------------------------------------
# _resolve_class — precedence order
# ---------------------------------------------------------------------------

class TestResolveClassOrder:
    def test_builtin_takes_priority(self, tmp_path, monkeypatch):
        """Built-in registry wins over system registry."""
        monkeypatch.setitem(SYSTEM_REGISTRY, "jira", type("FakeJira", (), {}))
        cls = _resolve_class("jira", {}, tmp_path)
        assert cls is BUILTIN_REGISTRY["jira"]

    def test_system_tool_found(self, tmp_path, monkeypatch):
        class FakeTool:
            pass
        monkeypatch.setitem(SYSTEM_REGISTRY, "my_tool", FakeTool)
        cls = _resolve_class("my_tool", {}, tmp_path)
        assert cls is FakeTool

    def test_model_local_fallback(self, tmp_path):
        """module/class config falls back to model-local loading."""
        tool_dir = tmp_path / "tools"
        tool_dir.mkdir()
        (tool_dir / "custom.py").write_text(textwrap.dedent("""\
            from tools.base import BaseTool
            class CustomTool(BaseTool):
                def run(self, command, params=None):
                    yield {}
                def list_commands(self):
                    return []
        """))
        cfg = {"module": "tools.custom", "class": "CustomTool"}
        cls = _resolve_class("custom", cfg, tmp_path)
        assert cls is not None
        assert cls.__name__ == "CustomTool"

    def test_unknown_tool_returns_none(self, tmp_path):
        cls = _resolve_class("nonexistent", {}, tmp_path)
        assert cls is None


# ---------------------------------------------------------------------------
# PARAMS inspection
# ---------------------------------------------------------------------------

class TestParamsInspection:
    def test_params_readable_from_class(self, tmp_path):
        _write_tool(tmp_path, "demo", _SIMPLE_TOOL)
        result = discover_system_tools([tmp_path])
        params = result["demo"].PARAMS
        assert "api_key" in params
        assert params["api_key"]["required"] is True
        assert params["api_key"]["credential"] is True

    def test_base_tool_has_empty_params(self):
        assert BaseTool.PARAMS == {}

    def test_builtin_tools_have_params_attr(self):
        for name, cls in BUILTIN_REGISTRY.items():
            assert hasattr(cls, "PARAMS"), f"Built-in tool {name} missing PARAMS"


# ---------------------------------------------------------------------------
# list_available_tools
# ---------------------------------------------------------------------------

class TestListAvailableTools:
    def test_includes_builtins(self):
        result = list_available_tools()
        names = {t["name"] for t in result}
        assert "jira" in names
        assert "git" in names

    def test_includes_system_tools(self, monkeypatch):
        class FakeTool(BaseTool):
            PARAMS = {"url": {"required": True, "description": "URL"}}
            def run(self, command, params=None):
                yield {}
            def list_commands(self):
                return []

        monkeypatch.setitem(SYSTEM_REGISTRY, "fake", FakeTool)
        result = list_available_tools()
        fake = next(t for t in result if t["name"] == "fake")
        assert fake["source"] == "system"
        assert "url" in fake["params"]

    def test_source_field(self):
        result = list_available_tools()
        for t in result:
            if t["name"] in BUILTIN_REGISTRY:
                assert t["source"] == "builtin"
