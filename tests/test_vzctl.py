# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for tools/vzctl.py — the model-as-code sync CLI."""

from pathlib import Path

from tools.vzctl import (
    SPECS,
    Action,
    ApiClient,
    ArtifactSpec,
    sync_artifact,
    sync_model,
)

# ---------------------------------------------------------------------------
# Fake API client — records calls, returns canned responses
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text or str(payload or "")

    def json(self) -> dict:
        return self._payload


class FakeApiClient:
    """Records (verb, url_suffix, body) and returns canned responses."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []
        # Map url_suffix → response queue
        self.gets: dict[str, FakeResponse] = {}
        self.puts: dict[str, FakeResponse] = {}

    def get(self, suffix: str) -> FakeResponse:
        self.calls.append(("GET", suffix, None))
        return self.gets.get(suffix, FakeResponse(404))

    def put(self, suffix: str, body: dict) -> FakeResponse:
        self.calls.append(("PUT", suffix, body))
        return self.puts.get(suffix, FakeResponse(200, {}))


def _spec(kind: str) -> ArtifactSpec:
    return next(s for s in SPECS if s.kind == kind)


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


# ---------------------------------------------------------------------------
# sync_artifact — single-file semantics
# ---------------------------------------------------------------------------


def test_creates_new_artifact_when_server_returns_404(tmp_path):
    client = FakeApiClient()
    f = _write(tmp_path / "ontology" / "Widget.yaml", "name: Widget\nattributes: {}\n")
    action = sync_artifact(client, _spec("entity"), f, dry_run=False)
    assert action == Action(kind="entity", name="Widget", status="created")
    assert ("PUT", "/entity/Widget/yaml", {"content": "name: Widget\nattributes: {}\n"}) in client.calls


def test_unchanged_when_server_returns_identical_content(tmp_path):
    yaml = "name: Widget\nattributes: {}\n"
    client = FakeApiClient()
    client.gets["/entity/Widget"] = FakeResponse(200, {"raw_yaml": yaml})
    f = _write(tmp_path / "ontology" / "Widget.yaml", yaml)
    action = sync_artifact(client, _spec("entity"), f, dry_run=False)
    assert action.status == "unchanged"
    # No PUT issued
    assert not any(c[0] == "PUT" for c in client.calls)


def test_updates_when_content_differs(tmp_path):
    client = FakeApiClient()
    client.gets["/entity/Widget"] = FakeResponse(200, {"raw_yaml": "name: Widget\nold: 1\n"})
    f = _write(tmp_path / "ontology" / "Widget.yaml", "name: Widget\nnew: 1\n")
    action = sync_artifact(client, _spec("entity"), f, dry_run=False)
    assert action.status == "updated"


def test_dry_run_does_not_put(tmp_path):
    client = FakeApiClient()
    f = _write(tmp_path / "ontology" / "Widget.yaml", "name: Widget\n")
    action = sync_artifact(client, _spec("entity"), f, dry_run=True)
    assert action.status == "created"
    assert not any(c[0] == "PUT" for c in client.calls)


def test_put_failure_returns_failed_with_message(tmp_path):
    client = FakeApiClient()
    client.puts["/entity/Widget/yaml"] = FakeResponse(
        422, {"detail": "Entity validation failed"}, text='{"detail":"Entity validation failed"}',
    )
    f = _write(tmp_path / "ontology" / "Widget.yaml", "name: Widget\n")
    action = sync_artifact(client, _spec("entity"), f, dry_run=False)
    assert action.status == "failed"
    assert "422" in action.error
    assert "Entity validation failed" in action.error


def test_invalid_yaml_yields_failed(tmp_path):
    client = FakeApiClient()
    f = _write(tmp_path / "ontology" / "Widget.yaml", ":\nnotvalid:\n  -")
    action = sync_artifact(client, _spec("entity"), f, dry_run=False)
    assert action.status == "failed"


def test_non_mapping_yaml_yields_failed(tmp_path):
    client = FakeApiClient()
    f = _write(tmp_path / "ontology" / "Widget.yaml", "- just\n- a\n- list\n")
    action = sync_artifact(client, _spec("entity"), f, dry_run=False)
    assert action.status == "failed"
    assert "mapping" in action.error


# ---------------------------------------------------------------------------
# Per-artifact-type wiring — extra path parts, name extraction, stem stripping
# ---------------------------------------------------------------------------


def test_feature_uses_feature_id_from_yaml(tmp_path):
    client = FakeApiClient()
    yaml = (
        "feature_id: issue.resolved_at\n"
        "name: Resolved At\n"
        "entity_type: Issue\n"
        "data_type: STRING\n"
    )
    f = _write(tmp_path / "features" / "anything.yaml", yaml)
    action = sync_artifact(client, _spec("feature"), f, dry_run=False)
    assert action.name == "issue.resolved_at"
    assert action.status == "created"
    assert ("PUT", "/entity/Issue/feature/issue.resolved_at",
            {"content": yaml}) in client.calls


def test_feature_missing_entity_type_yields_failed(tmp_path):
    client = FakeApiClient()
    f = _write(tmp_path / "features" / "x.yaml",
               "feature_id: foo.bar\n# no entity_type\n")
    action = sync_artifact(client, _spec("feature"), f, dry_run=False)
    assert action.status == "failed"
    assert "entity" in action.error


def test_extractor_strips_filename_prefix(tmp_path):
    client = FakeApiClient()
    f = _write(tmp_path / "extractors" / "extractor_jira.yaml",
               "tasks: []\n")
    action = sync_artifact(client, _spec("extractor"), f, dry_run=False)
    assert action.name == "jira"
    assert ("PUT", "/tool/jira/extract", {"content": "tasks: []\n"}) in client.calls


# ---------------------------------------------------------------------------
# sync_model — full directory walk
# ---------------------------------------------------------------------------


def test_sync_model_walks_all_artifact_dirs(tmp_path):
    """One file of each kind end-to-end."""
    client = FakeApiClient()
    _write(tmp_path / "ontology" / "Widget.yaml", "name: Widget\n")
    _write(tmp_path / "mappers" / "widget.yaml", "mapper: widget\n")
    _write(tmp_path / "features" / "f.yaml",
           "feature_id: widget.colour\nentity_type: Widget\ndata_type: STRING\n")
    _write(tmp_path / "queries" / "q.yaml", "name: q\n")
    _write(tmp_path / "views" / "v.yaml", "name: v\n")
    _write(tmp_path / "applications" / "a.yaml", "name: a\n")
    _write(tmp_path / "extractors" / "extractor_jira.yaml", "tasks: []\n")

    result = sync_model(client, tmp_path, dry_run=True)

    kinds = sorted(a.kind for a in result.actions)
    assert kinds == sorted(["entity", "mapper", "feature", "query", "view", "application", "extractor"])
    # Entity first (sync order matters — mappers / features / queries may reference entities)
    assert result.actions[0].kind == "entity"


def test_sync_model_missing_subdirs_are_skipped(tmp_path):
    """A model with only ontology/ doesn't error on absent mapper/ etc."""
    client = FakeApiClient()
    _write(tmp_path / "ontology" / "Widget.yaml", "name: Widget\n")
    result = sync_model(client, tmp_path, dry_run=True)
    assert len(result.actions) == 1
    assert result.actions[0].kind == "entity"


def test_sync_result_categorises_actions(tmp_path):
    client = FakeApiClient()
    # New
    _write(tmp_path / "ontology" / "A.yaml", "name: A\n")
    # Unchanged
    same = "name: B\n"
    _write(tmp_path / "ontology" / "B.yaml", same)
    client.gets["/entity/B"] = FakeResponse(200, {"raw_yaml": same})
    # Updated
    _write(tmp_path / "ontology" / "C.yaml", "name: C\nnew: 1\n")
    client.gets["/entity/C"] = FakeResponse(200, {"raw_yaml": "name: C\nold: 1\n"})
    # Failed
    _write(tmp_path / "ontology" / "D.yaml", "name: D\n")
    client.puts["/entity/D/yaml"] = FakeResponse(500, text="server exploded")

    result = sync_model(client, tmp_path, dry_run=False)
    assert [a.name for a in result.created] == ["A"]
    assert [a.name for a in result.unchanged] == ["B"]
    assert [a.name for a in result.updated] == ["C"]
    assert [a.name for a in result.failed] == ["D"]


# ---------------------------------------------------------------------------
# ApiClient — URL construction + header
# ---------------------------------------------------------------------------


def test_api_client_constructs_urls():
    client = ApiClient("https://vizgrams.com/", "vzsa_secret", "iagai")
    assert client._url("/entity/Widget") == "https://vizgrams.com/api/v1/model/iagai/entity/Widget"
    assert client.session.headers["X-API-Key"] == "vzsa_secret"


def test_api_client_strips_trailing_slash_from_base():
    client = ApiClient("https://vizgrams.com/", "k", "m")
    assert "//api/" not in client._url("/x")
