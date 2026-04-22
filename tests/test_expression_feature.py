# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Integration tests: expression feature produces equivalent output to raw_sql (ADR-116 Phase 3)."""

import sqlite3
from pathlib import Path

from semantic.expression import AggExpr, AggFunc, CaseWhenExpr, ExpressionFeatureDef, FieldRef
from semantic.feature import FeatureDef, load_feature_yamls, reconcile
from semantic.types import (
    AttributeDef,
    Cardinality,
    ColumnType,
    EntityDef,
    RelationDef,
    SemanticHint,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'test.db'}"


def _connect(tmp_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(str(tmp_path / "test.db"))


def _sprint_entity() -> EntityDef:
    return EntityDef(
        name="Sprint",
        identity=[AttributeDef("sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        attributes=[
            AttributeDef("start_date", ColumnType.STRING),
        ],
    )


def _team_sprint_entity() -> EntityDef:
    return EntityDef(
        name="TeamSprint",
        identity=[AttributeDef("team_sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        attributes=[
            AttributeDef("sprint_key", ColumnType.STRING, SemanticHint.RELATION),
            AttributeDef("score", ColumnType.FLOAT),
        ],
        relations=[
            RelationDef(
                name="sprint",
                target="Sprint",
                via="sprint_key",
                cardinality=Cardinality.MANY_TO_ONE,
            )
        ],
    )


def _setup_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE sprint (
            sprint_key TEXT PRIMARY KEY,
            start_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE team_sprint (
            team_sprint_key TEXT PRIMARY KEY,
            sprint_key TEXT,
            score REAL
        )
    """)
    conn.executemany(
        "INSERT INTO sprint VALUES (?, ?)",
        [("s1", "2024-01-01"), ("s2", "2024-02-01")],
    )
    conn.executemany(
        "INSERT INTO team_sprint VALUES (?, ?, ?)",
        [("t1_s1", "s1", 10.0), ("t1_s2", "s2", 20.0), ("t2_s1", "s1", 5.0)],
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestExpressionEquivalentToRawSql:
    """Expression feature and raw_sql feature produce identical feature_value rows."""

    def _raw_sql_fd(self) -> FeatureDef:
        return FeatureDef(
            feature_id="team_sprint.start_date",
            name="Team Sprint Start Date",
            entity_type="TeamSprint",
            entity_key="team_sprint_key",
            data_type="STRING",
            materialization_mode="materialized",
            raw_sql="""
SELECT
    ts.team_sprint_key AS entity_id,
    sp.start_date AS value
FROM team_sprint ts
LEFT JOIN sprint sp ON sp.sprint_key = ts.sprint_key
""",
        )

    def _expr_fd(self) -> ExpressionFeatureDef:
        return ExpressionFeatureDef(
            feature_id="team_sprint.start_date",
            name="Team Sprint Start Date",
            entity_type="TeamSprint",
            entity_key="team_sprint_key",
            data_type="STRING",
            materialization_mode="materialized",
            expression=FieldRef(["Sprint", "start_date"]),
        )

    def _reconcile_and_fetch(self, tmp_path: Path, fd, entities: dict) -> dict[str, str | None]:
        conn = _connect(tmp_path)
        _setup_db(conn)
        conn.close()
        reconcile([fd], entities, _db_url(tmp_path))
        conn = _connect(tmp_path)
        rows = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT entity_id, value FROM __feature_value WHERE feature_id=?",
                (fd.feature_id,),
            )
        }
        conn.close()
        return rows

    def test_raw_sql_produces_correct_rows(self, tmp_path):
        entities = {
            "TeamSprint": _team_sprint_entity(),
            "Sprint": _sprint_entity(),
        }
        rows = self._reconcile_and_fetch(tmp_path, self._raw_sql_fd(), entities)
        assert rows["t1_s1"] == "2024-01-01"
        assert rows["t1_s2"] == "2024-02-01"
        assert rows["t2_s1"] == "2024-01-01"

    def test_expression_produces_same_rows(self, tmp_path):
        entities = {
            "TeamSprint": _team_sprint_entity(),
            "Sprint": _sprint_entity(),
        }
        rows = self._reconcile_and_fetch(tmp_path, self._expr_fd(), entities)
        assert rows["t1_s1"] == "2024-01-01"
        assert rows["t1_s2"] == "2024-02-01"
        assert rows["t2_s1"] == "2024-01-01"

    def test_expression_row_count_matches_raw_sql(self, tmp_path):
        entities = {
            "TeamSprint": _team_sprint_entity(),
            "Sprint": _sprint_entity(),
        }
        raw_dir = tmp_path / "raw"
        expr_dir = tmp_path / "expr"
        raw_dir.mkdir()
        expr_dir.mkdir()
        raw_rows = self._reconcile_and_fetch(raw_dir, self._raw_sql_fd(), entities)
        expr_rows = self._reconcile_and_fetch(expr_dir, self._expr_fd(), entities)
        assert len(raw_rows) == len(expr_rows)
        assert set(raw_rows.keys()) == set(expr_rows.keys())
        for k in raw_rows:
            assert raw_rows[k] == expr_rows[k], f"Mismatch for entity_id={k!r}"


class TestExpressionYamlLoading:
    """Verify that expression: YAML key loads as ExpressionFeatureDef."""

    def test_expression_yaml_loads_as_expression_feature(self, tmp_path):
        feat_dir = tmp_path / "features"
        feat_dir.mkdir()
        (feat_dir / "team_sprint.start_date.yaml").write_text(
            "feature_id: team_sprint.start_date\n"
            "name: Team Sprint Start Date\n"
            "entity_type: TeamSprint\n"
            "entity_key: team_sprint_key\n"
            "data_type: STRING\n"
            "materialization_mode: materialized\n"
            "expr: Sprint.start_date\n"
        )
        fds = load_feature_yamls(feat_dir)
        assert len(fds) == 1
        fd = fds[0]
        assert fd.feature_type == "expression"
        assert isinstance(fd.expression, FieldRef)
        assert fd.expression.parts == ["Sprint", "start_date"]

    def test_raw_sql_yaml_loads_as_raw_sql_feature(self, tmp_path):
        feat_dir = tmp_path / "features"
        feat_dir.mkdir()
        (feat_dir / "widget.score.yaml").write_text(
            "feature_id: widget.score\n"
            "name: Score\n"
            "entity_type: Widget\n"
            "entity_key: widget_key\n"
            "data_type: FLOAT\n"
            "materialization_mode: materialized\n"
            "raw_sql: |\n"
            "  SELECT widget_key AS entity_id, value AS value FROM widget\n"
        )
        fds = load_feature_yamls(feat_dir)
        assert len(fds) == 1
        fd = fds[0]
        assert fd.feature_type == "raw_sql"

    def test_expression_feature_reconciles_end_to_end(self, tmp_path):
        feat_dir = tmp_path / "features"
        feat_dir.mkdir()
        (feat_dir / "team_sprint.start_date.yaml").write_text(
            "feature_id: team_sprint.start_date\n"
            "name: Team Sprint Start Date\n"
            "entity_type: TeamSprint\n"
            "entity_key: team_sprint_key\n"
            "data_type: STRING\n"
            "materialization_mode: materialized\n"
            "expr: Sprint.start_date\n"
        )
        entities = {
            "TeamSprint": _team_sprint_entity(),
            "Sprint": _sprint_entity(),
        }
        conn = _connect(tmp_path)
        _setup_db(conn)
        conn.close()

        fds = load_feature_yamls(feat_dir)
        reconcile(fds, entities, _db_url(tmp_path))

        conn = _connect(tmp_path)
        rows = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT entity_id, value FROM __feature_value WHERE feature_id=?",
                ("team_sprint.start_date",),
            )
        }
        conn.close()
        assert rows["t1_s1"] == "2024-01-01"
        assert rows["t2_s1"] == "2024-01-01"


# ---------------------------------------------------------------------------
# Phase 4: Migrated YAML files load as ExpressionFeatureDef
# ---------------------------------------------------------------------------

class TestPhase4MigratedFeatures:
    """Verify the two start_date features were migrated to expr: key."""

    _FEATURES_DIR = (
        Path(__file__).resolve().parent.parent
        / "models" / "example" / "features"
    )

    def setup_method(self):
        from core.metadata_db import seed_from_directory
        seed_from_directory(self._FEATURES_DIR.parent)

    def _load(self):
        from semantic.yaml_adapter import YAMLAdapter
        return YAMLAdapter.load_features(self._FEATURES_DIR)

    def test_team_sprint_start_date_is_expression(self):
        fds = self._load()
        fd = next(f for f in fds if f.feature_id == "team_sprint.start_date")
        assert fd.feature_type == "expression"
        assert isinstance(fd.expression, FieldRef)
        assert fd.expression.parts == ["Sprint", "start_date"]

    def test_product_sprint_start_date_is_expression(self):
        fds = self._load()
        fd = next(f for f in fds if f.feature_id == "product_sprint.start_date")
        assert fd.feature_type == "expression"
        assert isinstance(fd.expression, FieldRef)
        assert fd.expression.parts == ["Sprint", "start_date"]

    def test_example_features_all_expression_type(self):
        """All features in the example model use the expr: key (no raw_sql)."""
        fds = self._load()
        expression_ids = {f.feature_id for f in fds if f.feature_type == "expression"}
        assert expression_ids == {
            "team_sprint.start_date",
            "product_sprint.start_date",
            "team_sprint.completed_sp",
            "team_sprint.not_completed_sp",
            "team_sprint.removed_sp",
        }
        raw_sql_ids = {f.feature_id for f in fds if f.feature_type == "raw_sql"}
        assert len(raw_sql_ids) == 0

    def test_team_sprint_completed_sp_is_case_when_expr(self):
        fds = self._load()
        fd = next(f for f in fds if f.feature_id == "team_sprint.completed_sp")
        assert fd.feature_type == "expression"
        # sum(case when outcome = 'completed' then SprintIssue.story_points end)
        assert isinstance(fd.expression, AggExpr)
        assert fd.expression.func == AggFunc.SUM
        assert isinstance(fd.expression.expr, CaseWhenExpr)

    def test_team_sprint_not_completed_sp_is_case_when_expr(self):
        fds = self._load()
        fd = next(f for f in fds if f.feature_id == "team_sprint.not_completed_sp")
        assert isinstance(fd.expression, AggExpr)
        assert isinstance(fd.expression.expr, CaseWhenExpr)

    def test_team_sprint_removed_sp_is_case_when_expr(self):
        fds = self._load()
        fd = next(f for f in fds if f.feature_id == "team_sprint.removed_sp")
        assert isinstance(fd.expression, AggExpr)
        assert isinstance(fd.expression.expr, CaseWhenExpr)


# ---------------------------------------------------------------------------
# CaseWhenExpr / ONE_TO_MANY integration tests
# ---------------------------------------------------------------------------

def _sprint_issue_entity() -> EntityDef:
    return EntityDef(
        name="SprintIssue",
        identity=[AttributeDef("sprint_issue_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        attributes=[
            AttributeDef("team_key", ColumnType.STRING, SemanticHint.RELATION),
            AttributeDef("sprint_key", ColumnType.STRING, SemanticHint.RELATION),
            AttributeDef("story_points", ColumnType.FLOAT, SemanticHint.MEASURE),
            AttributeDef("outcome", ColumnType.STRING),
        ],
    )


def _team_sprint_with_relation() -> EntityDef:
    return EntityDef(
        name="TeamSprint",
        identity=[AttributeDef("team_sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        attributes=[
            AttributeDef("team_key", ColumnType.STRING, SemanticHint.RELATION),
            AttributeDef("sprint_key", ColumnType.STRING, SemanticHint.RELATION),
        ],
        relations=[
            RelationDef(
                name="sprint_issues",
                target="SprintIssue",
                cardinality=Cardinality.ONE_TO_MANY,
                via=["team_key", "sprint_key"],
            )
        ],
    )


def _setup_sprint_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE team_sprint (
            team_sprint_key TEXT PRIMARY KEY,
            team_key TEXT,
            sprint_key TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE sprint_issue (
            sprint_issue_key TEXT PRIMARY KEY,
            team_key TEXT,
            sprint_key TEXT,
            story_points REAL,
            outcome TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO team_sprint VALUES (?, ?, ?)",
        [("t1_s1", "t1", "s1"), ("t2_s1", "t2", "s1")],
    )
    conn.executemany(
        "INSERT INTO sprint_issue VALUES (?, ?, ?, ?, ?)",
        [
            ("si1", "t1", "s1", 3.0, "completed"),
            ("si2", "t1", "s1", 2.0, "completed"),
            ("si3", "t1", "s1", 5.0, "incomplete"),
            ("si4", "t1", "s1", 1.0, "punted"),
            ("si5", "t2", "s1", 4.0, "completed"),
        ],
    )
    conn.commit()


def _make_case_when_fd(feature_id: str, name: str, expression: str) -> ExpressionFeatureDef:
    from semantic.expression import parse_expression_str
    return ExpressionFeatureDef(
        feature_id=feature_id,
        name=name,
        entity_type="TeamSprint",
        entity_key="team_sprint_key",
        data_type="FLOAT",
        materialization_mode="materialized",
        expression=parse_expression_str(expression),
    )


class TestCaseWhenExprIntegration:
    """CASE WHEN expressions with ONE_TO_MANY joins compile and reconcile correctly."""

    def setup_method(self):
        from pathlib import Path

        from core.metadata_db import seed_from_directory
        model_dir = Path(__file__).resolve().parent.parent / "models" / "example"
        seed_from_directory(model_dir)

    def _entities(self) -> dict:
        return {
            "TeamSprint": _team_sprint_with_relation(),
            "SprintIssue": _sprint_issue_entity(),
        }

    def _reconcile_feature(
        self, tmp_path: Path, fd, entities: dict
    ) -> dict[str, str | None]:
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        _setup_sprint_db(conn)
        conn.close()
        reconcile([fd], entities, f"sqlite:///{tmp_path / 'test.db'}")
        conn = sqlite3.connect(str(tmp_path / "test.db"))
        rows = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT entity_id, value FROM __feature_value WHERE feature_id=?",
                (fd.feature_id,),
            )
        }
        conn.close()
        return rows

    def test_completed_sp_sums_correct_rows(self, tmp_path):
        fd = _make_case_when_fd(
            "team_sprint.completed_sp", "Completed SP",
            "sum(case when outcome = 'completed' then SprintIssue.story_points end)",
        )
        rows = self._reconcile_feature(tmp_path, fd, self._entities())
        assert float(rows["t1_s1"]) == 5.0   # si1 (3) + si2 (2)
        assert float(rows["t2_s1"]) == 4.0   # si5

    def test_not_completed_sp_sums_correct_rows(self, tmp_path):
        fd = _make_case_when_fd(
            "team_sprint.not_completed_sp", "Not Completed SP",
            "sum(case when outcome = 'incomplete' then SprintIssue.story_points end)",
        )
        rows = self._reconcile_feature(tmp_path, fd, self._entities())
        assert float(rows["t1_s1"]) == 5.0   # si3
        assert rows.get("t2_s1") is None     # no incomplete rows for t2 → NULL → not stored

    def test_removed_sp_sums_correct_rows(self, tmp_path):
        fd = _make_case_when_fd(
            "team_sprint.removed_sp", "Removed SP",
            "sum(case when outcome = 'punted' then SprintIssue.story_points end)",
        )
        rows = self._reconcile_feature(tmp_path, fd, self._entities())
        assert float(rows["t1_s1"]) == 1.0   # si4
        assert rows.get("t2_s1") is None     # no punted rows for t2 → NULL

    def test_case_when_with_else_zero(self, tmp_path):
        fd = _make_case_when_fd(
            "team_sprint.removed_sp", "Removed SP",
            "sum(case when outcome = 'punted' then SprintIssue.story_points else 0 end)",
        )
        rows = self._reconcile_feature(tmp_path, fd, self._entities())
        assert float(rows["t1_s1"]) == 1.0
        assert float(rows["t2_s1"]) == 0.0   # ELSE 0 → explicitly 0

    def test_bare_field_resolves_via_one_to_many(self, tmp_path):
        """Bare 'outcome' resolves to SprintIssue.outcome via lazy join."""
        fd = _make_case_when_fd(
            "team_sprint.completed_sp", "Completed SP",
            "sum(case when outcome = 'completed' then SprintIssue.story_points end)",
        )
        rows = self._reconcile_feature(tmp_path, fd, self._entities())
        assert float(rows["t1_s1"]) == 5.0

    def test_yaml_feature_files_parse_as_agg_case_when(self):
        """The migrated YAML files parse as AggExpr(CaseWhenExpr(...))."""
        from semantic.yaml_adapter import YAMLAdapter
        feat_dir = (
            Path(__file__).resolve().parent.parent
            / "models" / "example" / "features"
        )
        fds = YAMLAdapter.load_features(feat_dir)
        sp_features = {
            f.feature_id: f for f in fds
            if f.feature_id in {
                "team_sprint.completed_sp",
                "team_sprint.not_completed_sp",
                "team_sprint.removed_sp",
            }
        }
        assert len(sp_features) == 3
        for fd in sp_features.values():
            assert isinstance(fd.expression, AggExpr)
            assert isinstance(fd.expression.expr, CaseWhenExpr)
