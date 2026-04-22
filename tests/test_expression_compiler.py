# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for semantic.expression and engine.expression_compiler (ADR-116 Phase 3)."""

import pytest

from engine.expression_compiler import (
    CompileContext,
    _has_aggregation,
    _has_window,
    compile_expr,
    compile_feature_to_sql,
)
from semantic.expression import (
    AggExpr,
    AggFunc,
    BinOp,
    CaseWhenExpr,
    DurationLit,
    ExpressionFeatureDef,
    FieldRef,
    FuncCallExpr,
    InExpr,
    ListLit,
    Lit,
    MethodCallExpr,
    UnaryExpr,
    WindowExpr,
    parse_expression_str,
)
from semantic.types import AttributeDef, Cardinality, ColumnType, EntityDef, RelationDef, SemanticHint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sprint_entity() -> EntityDef:
    return EntityDef(
        name="Sprint",
        identity=[AttributeDef("sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        attributes=[
            AttributeDef("start_date", ColumnType.STRING),
            AttributeDef("story_points", ColumnType.FLOAT),
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


def _make_ctx(root_entity: EntityDef, entities: dict | None = None) -> CompileContext:
    if entities is None:
        entities = {}
    return CompileContext(
        root_entity=root_entity,
        root_alias="root",
        entities=entities,
        join_steps=[],
        joined={},
    )


# ---------------------------------------------------------------------------
# parse_expression_str tests
# ---------------------------------------------------------------------------

class TestParseExpressionStr:
    def test_single_field(self):
        expr = parse_expression_str("score")
        assert isinstance(expr, FieldRef)
        assert expr.parts == ["score"]

    def test_dotted_field(self):
        expr = parse_expression_str("Sprint.start_date")
        assert isinstance(expr, FieldRef)
        assert expr.parts == ["Sprint", "start_date"]

    def test_number_literal(self):
        expr = parse_expression_str("42")
        assert isinstance(expr, Lit)
        assert expr.value == 42

    def test_float_literal(self):
        expr = parse_expression_str("3.14")
        assert isinstance(expr, Lit)
        assert abs(expr.value - 3.14) < 1e-9

    def test_string_literal(self):
        expr = parse_expression_str("'hello'")
        assert isinstance(expr, Lit)
        assert expr.value == "hello"

    def test_null_literal(self):
        expr = parse_expression_str("null")
        assert isinstance(expr, Lit)
        assert expr.value is None

    def test_agg_sum(self):
        expr = parse_expression_str("sum(score)")
        assert isinstance(expr, AggExpr)
        assert expr.func == AggFunc.SUM
        assert isinstance(expr.expr, FieldRef)

    def test_agg_count_distinct(self):
        expr = parse_expression_str("count_distinct(issue_key)")
        assert isinstance(expr, AggExpr)
        assert expr.func == AggFunc.COUNT_DISTINCT

    def test_agg_count_no_args(self):
        expr = parse_expression_str("count()")
        assert isinstance(expr, AggExpr)
        assert expr.func == AggFunc.COUNT
        assert isinstance(expr.expr, Lit)
        assert expr.expr.value == "*"

    def test_binop_division(self):
        expr = parse_expression_str("a / b")
        assert isinstance(expr, BinOp)
        assert expr.op == "/"

    def test_binop_addition(self):
        expr = parse_expression_str("a + b")
        assert isinstance(expr, BinOp)
        assert expr.op == "+"

    def test_nested_arithmetic(self):
        expr = parse_expression_str("(a + b) / c")
        assert isinstance(expr, BinOp)
        assert expr.op == "/"
        assert isinstance(expr.left, BinOp)
        assert expr.left.op == "+"

    def test_is_null(self):
        expr = parse_expression_str("score is null")
        assert isinstance(expr, UnaryExpr)
        assert expr.op == "is_null"

    def test_is_not_null(self):
        expr = parse_expression_str("score is not null")
        assert isinstance(expr, UnaryExpr)
        assert expr.op == "is_not_null"

    def test_unary_not(self):
        expr = parse_expression_str("not score")
        assert isinstance(expr, UnaryExpr)
        assert expr.op == "not"

    def test_func_call(self):
        expr = parse_expression_str("coalesce(score, 0)")
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "coalesce"
        assert len(expr.args) == 2

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_expression_str("")

    def test_invalid_token_raises(self):
        with pytest.raises(ValueError):
            parse_expression_str("a + @b")


# ---------------------------------------------------------------------------
# compile_expr — Lit
# ---------------------------------------------------------------------------

class TestCompileLit:
    def _ctx(self):
        entity = EntityDef(name="X", identity=[], attributes=[])
        return _make_ctx(entity)

    def test_none_is_null(self):
        assert compile_expr(Lit(None), self._ctx()) == "NULL"

    def test_true_is_1(self):
        assert compile_expr(Lit(True), self._ctx()) == "1"

    def test_false_is_0(self):
        assert compile_expr(Lit(False), self._ctx()) == "0"

    def test_integer(self):
        assert compile_expr(Lit(42), self._ctx()) == "42"

    def test_float(self):
        assert compile_expr(Lit(3.14), self._ctx()) == "3.14"

    def test_string(self):
        assert compile_expr(Lit("hello"), self._ctx()) == "'hello'"

    def test_string_with_single_quote_escaped(self):
        assert compile_expr(Lit("it's"), self._ctx()) == "'it''s'"

    def test_star(self):
        assert compile_expr(Lit("*"), self._ctx()) == "*"


# ---------------------------------------------------------------------------
# compile_expr — BinOp
# ---------------------------------------------------------------------------

class TestCompileBinOp:
    def _ctx(self):
        entity = EntityDef(name="X", identity=[], attributes=[])
        return _make_ctx(entity)

    def test_addition(self):
        expr = BinOp(left=Lit(1), op="+", right=Lit(2))
        assert compile_expr(expr, self._ctx()) == "(1 + 2)"

    def test_subtraction(self):
        expr = BinOp(left=Lit(5), op="-", right=Lit(3))
        assert compile_expr(expr, self._ctx()) == "(5 - 3)"

    def test_multiplication(self):
        expr = BinOp(left=Lit(2), op="*", right=Lit(3))
        assert compile_expr(expr, self._ctx()) == "(2 * 3)"

    def test_division_wraps_nullif(self):
        """Division must wrap denominator in NULLIF(expr, 0) (ADR 5.13.3)."""
        expr = BinOp(left=Lit(10), op="/", right=Lit(2))
        result = compile_expr(expr, self._ctx())
        assert "NULLIF" in result
        assert result == "(10 / NULLIF(2, 0))"

    def test_division_with_field_denominator(self):
        """Division with field denominator wraps field in NULLIF."""
        entity = EntityDef(
            name="Widget",
            identity=[AttributeDef("widget_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("total", ColumnType.FLOAT), AttributeDef("count", ColumnType.FLOAT)],
        )
        ctx = _make_ctx(entity)
        expr = BinOp(left=FieldRef(["total"]), op="/", right=FieldRef(["count"]))
        result = compile_expr(expr, ctx)
        assert "NULLIF(root.count, 0)" in result

    def test_and_operator(self):
        expr = BinOp(left=Lit(True), op="AND", right=Lit(False))
        assert compile_expr(expr, self._ctx()) == "(1 AND 0)"

    def test_or_operator(self):
        expr = BinOp(left=Lit(True), op="OR", right=Lit(False))
        assert compile_expr(expr, self._ctx()) == "(1 OR 0)"

    def test_parse_and_precedence(self):
        """a = 1 and b = 2 should parse as (a=1) AND (b=2)."""
        expr = parse_expression_str("a = 1 and b = 2")
        assert isinstance(expr, BinOp)
        assert expr.op == "AND"
        assert isinstance(expr.left, BinOp) and expr.left.op == "="
        assert isinstance(expr.right, BinOp) and expr.right.op == "="

    def test_parse_or_precedence(self):
        """a = 1 or b = 2 should parse as (a=1) OR (b=2)."""
        expr = parse_expression_str("a = 1 or b = 2")
        assert isinstance(expr, BinOp)
        assert expr.op == "OR"

    def test_parse_and_with_is_null(self):
        """x is null and y = 1 — IS NULL binds tighter than AND."""
        expr = parse_expression_str("x is null and y = 1")
        assert isinstance(expr, BinOp)
        assert expr.op == "AND"
        assert isinstance(expr.left, UnaryExpr) and expr.left.op == "is_null"
        assert isinstance(expr.right, BinOp) and expr.right.op == "="

    def test_parse_chained_and(self):
        expr = parse_expression_str("a = 1 and b = 2 and c = 3")
        # left-associative: ((a=1) AND (b=2)) AND (c=3)
        assert isinstance(expr, BinOp) and expr.op == "AND"
        assert isinstance(expr.left, BinOp) and expr.left.op == "AND"


# ---------------------------------------------------------------------------
# compile_expr — AggExpr
# ---------------------------------------------------------------------------

class TestCompileAggExpr:
    def _ctx(self):
        entity = EntityDef(name="X", identity=[], attributes=[])
        return _make_ctx(entity)

    def test_sum(self):
        expr = AggExpr(func=AggFunc.SUM, expr=Lit(1))
        assert compile_expr(expr, self._ctx()) == "SUM(1)"

    def test_avg(self):
        expr = AggExpr(func=AggFunc.AVG, expr=FieldRef(["score"]))
        assert "AVG" in compile_expr(expr, _make_ctx(
            EntityDef(name="X", identity=[], attributes=[])
        ))

    def test_count_star(self):
        expr = AggExpr(func=AggFunc.COUNT, expr=Lit("*"))
        assert compile_expr(expr, self._ctx()) == "COUNT(*)"

    def test_count_distinct(self):
        expr = AggExpr(func=AggFunc.COUNT_DISTINCT, expr=FieldRef(["issue_key"]))
        result = compile_expr(expr, _make_ctx(
            EntityDef(name="X", identity=[], attributes=[])
        ))
        assert result == "COUNT(DISTINCT root.issue_key)"

    def test_min(self):
        expr = AggExpr(func=AggFunc.MIN, expr=FieldRef(["score"]))
        assert compile_expr(expr, self._ctx()).startswith("MIN(")

    def test_max(self):
        expr = AggExpr(func=AggFunc.MAX, expr=FieldRef(["score"]))
        assert compile_expr(expr, self._ctx()).startswith("MAX(")


# ---------------------------------------------------------------------------
# compile_expr — UnaryExpr
# ---------------------------------------------------------------------------

class TestCompileUnaryExpr:
    def _ctx(self):
        entity = EntityDef(name="X", identity=[], attributes=[])
        return _make_ctx(entity)

    def test_is_null(self):
        expr = UnaryExpr(op="is_null", expr=FieldRef(["score"]))
        assert compile_expr(expr, self._ctx()) == "(root.score IS NULL)"

    def test_is_not_null(self):
        expr = UnaryExpr(op="is_not_null", expr=FieldRef(["score"]))
        assert compile_expr(expr, self._ctx()) == "(root.score IS NOT NULL)"

    def test_not(self):
        expr = UnaryExpr(op="not", expr=Lit(True))
        assert compile_expr(expr, self._ctx()) == "(NOT 1)"

    def test_unary_minus(self):
        expr = UnaryExpr(op="-", expr=Lit(5))
        assert compile_expr(expr, self._ctx()) == "(-5)"


# ---------------------------------------------------------------------------
# compile_expr — FieldRef traversal (JOIN accumulation)
# ---------------------------------------------------------------------------

class TestFieldRefTraversal:
    def test_bare_field_no_join(self):
        entity = EntityDef(
            name="TeamSprint",
            identity=[AttributeDef("team_sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("score", ColumnType.FLOAT)],
        )
        ctx = _make_ctx(entity)
        result = compile_expr(FieldRef(["score"]), ctx)
        assert result == "root.score"
        assert len(ctx.join_steps) == 0

    def test_traversal_adds_join(self):
        sprint = _sprint_entity()
        ts = _team_sprint_entity()
        entities = {"Sprint": sprint, "TeamSprint": ts}
        ctx = CompileContext(
            root_entity=ts,
            root_alias="tea",
            entities=entities,
            join_steps=[],
            joined={},
        )
        result = compile_expr(FieldRef(["Sprint", "start_date"]), ctx)
        assert len(ctx.join_steps) == 1
        step = ctx.join_steps[0]
        assert step["from_alias"] == "tea"
        assert step["from_col"] == "sprint_key"
        assert step["target_table"] == "sprint"
        assert step["target_pk"] == "sprint_key"
        assert "start_date" in result

    def test_traversal_not_duplicated(self):
        """Second field ref to same entity should reuse the join."""
        sprint = _sprint_entity()
        ts = _team_sprint_entity()
        entities = {"Sprint": sprint, "TeamSprint": ts}
        ctx = CompileContext(
            root_entity=ts,
            root_alias="tea",
            entities=entities,
            join_steps=[],
            joined={},
        )
        compile_expr(FieldRef(["Sprint", "start_date"]), ctx)
        compile_expr(FieldRef(["Sprint", "story_points"]), ctx)
        assert len(ctx.join_steps) == 1  # only one JOIN added

    def test_unknown_relation_raises(self):
        entity = EntityDef(
            name="TeamSprint",
            identity=[AttributeDef("team_sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[],
            relations=[],
        )
        ctx = _make_ctx(entity, {})
        with pytest.raises(ValueError, match="No relation from 'TeamSprint' to 'Sprint'"):
            compile_expr(FieldRef(["Sprint", "start_date"]), ctx)


# ---------------------------------------------------------------------------
# datetime_diff function
# ---------------------------------------------------------------------------

class TestDatetimeDiff:
    def _ctx(self):
        entity = EntityDef(
            name="PullRequest",
            identity=[AttributeDef("pull_request_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[
                AttributeDef("created_at", ColumnType.STRING),
                AttributeDef("merged_at", ColumnType.STRING),
            ],
        )
        return _make_ctx(entity)

    # --- parser: kwargs round-trip ---

    def test_parse_datetime_diff_kwargs(self):
        expr = parse_expression_str('datetime_diff(created_at, merged_at, unit="hours")')
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "datetime_diff"
        assert len(expr.args) == 2
        assert isinstance(expr.kwargs.get("unit"), Lit)
        assert expr.kwargs["unit"].value == "hours"

    def test_parse_datetime_diff_all_units(self):
        for unit in ("seconds", "minutes", "hours", "days", "years"):
            expr = parse_expression_str(f'datetime_diff(a, b, unit="{unit}")')
            assert expr.kwargs["unit"].value == unit

    # --- compiler: sqlite ---

    def test_sqlite_hours(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": Lit("hours")},
        )
        sql = compile_expr(expr, self._ctx())
        assert "julianday" in sql
        assert "* 24" in sql

    def test_sqlite_days(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": Lit("days")},
        )
        sql = compile_expr(expr, self._ctx())
        assert "CAST" in sql
        assert "AS INTEGER" in sql

    def test_sqlite_seconds(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": Lit("seconds")},
        )
        sql = compile_expr(expr, self._ctx())
        assert "86400" in sql

    def test_sqlite_minutes(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": Lit("minutes")},
        )
        sql = compile_expr(expr, self._ctx())
        assert "1440" in sql

    def test_sqlite_years(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": Lit("years")},
        )
        sql = compile_expr(expr, self._ctx())
        assert "365.25" in sql

    # --- error cases ---

    def test_missing_unit_raises(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={},
        )
        with pytest.raises(ValueError, match="unit"):
            compile_expr(expr, self._ctx())

    def test_bad_unit_raises(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": Lit("weeks")},
        )
        with pytest.raises(ValueError, match="weeks"):
            compile_expr(expr, self._ctx())

    def test_wrong_arg_count_raises(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"])],
            kwargs={"unit": Lit("hours")},
        )
        with pytest.raises(ValueError, match="2"):
            compile_expr(expr, self._ctx())

    def test_unit_must_be_string_literal(self):
        expr = FuncCallExpr(
            name="datetime_diff",
            args=[FieldRef(["created_at"]), FieldRef(["merged_at"])],
            kwargs={"unit": FieldRef(["unit_col"])},
        )
        with pytest.raises(ValueError, match="string literal"):
            compile_expr(expr, self._ctx())


# ---------------------------------------------------------------------------
# WindowExpr — parser + compiler + two-phase feature SQL
# ---------------------------------------------------------------------------

class TestWindowExpr:
    def _pv_entity(self) -> EntityDef:
        """Minimal ProductVersion entity with a lifecycle_events ONE_TO_MANY relation."""
        pvle = EntityDef(
            name="ProductVersionLifecycleEvent",
            identity=[AttributeDef("lifecycle_event_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[
                AttributeDef("product_version_key", ColumnType.STRING, SemanticHint.RELATION),
                AttributeDef("to_lifecycle_state", ColumnType.STRING),
                AttributeDef("occurred_at", ColumnType.STRING, SemanticHint.TIMESTAMP),
            ],
            relations=[],
        )
        pv = EntityDef(
            name="ProductVersion",
            identity=[AttributeDef("product_version_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[
                AttributeDef("product_key", ColumnType.STRING, SemanticHint.RELATION),
            ],
            relations=[
                RelationDef(
                    name="lifecycle_events",
                    target="ProductVersionLifecycleEvent",
                    via=["product_version_key"],
                    cardinality=Cardinality.ONE_TO_MANY,
                )
            ],
        )
        return pv, pvle

    def _entities(self):
        pv, pvle = self._pv_entity()
        return {"ProductVersion": pv, "ProductVersionLifecycleEvent": pvle}

    def _released_at_feat(self):
        expr = parse_expression_str(
            "min(case when ProductVersionLifecycleEvent.to_lifecycle_state = 'READY_FOR_ADOPTION'"
            " then ProductVersionLifecycleEvent.occurred_at end)"
        )
        return ExpressionFeatureDef(
            feature_id="product_version.released_at",
            name="Released At",
            entity_type="ProductVersion",
            entity_key="product_version_key",
            data_type="STRING",
            materialization_mode="materialized",
            expression=expr,
        )

    def _days_feat(self):
        expr = parse_expression_str(
            'datetime_diff(lag(released_at).over(product_key, released_at), released_at, unit="days")'
        )
        return ExpressionFeatureDef(
            feature_id="product_version.days_since_prev_version",
            name="Days Since Prev Version",
            entity_type="ProductVersion",
            entity_key="product_version_key",
            data_type="INTEGER",
            materialization_mode="materialized",
            expression=expr,
        )

    # --- parser tests ---

    def test_parse_lag_basic(self):
        expr = parse_expression_str("lag(released_at).over(product_key, released_at)")
        assert isinstance(expr, WindowExpr)
        assert expr.func == "lag"
        assert expr.arg == FieldRef(parts=["released_at"])
        assert expr.partition_by == [FieldRef(parts=["product_key"])]
        assert expr.order_by == [FieldRef(parts=["released_at"])]

    def test_parse_multiple_partition_cols(self):
        expr = parse_expression_str("lag(x).over(a, b, x)")
        assert isinstance(expr, WindowExpr)
        assert expr.partition_by == [FieldRef(parts=["a"]), FieldRef(parts=["b"])]
        assert expr.order_by == [FieldRef(parts=["x"])]

    def test_parse_over_too_few_args_raises(self):
        with pytest.raises(ValueError, match="at least 2 arguments"):
            parse_expression_str("lag(x).over(y)")

    def test_parse_nested_in_datetime_diff(self):
        expr = parse_expression_str(
            'datetime_diff(lag(released_at).over(product_key, released_at), released_at, unit="days")'
        )
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "datetime_diff"
        assert isinstance(expr.args[0], WindowExpr)
        assert expr.args[0].func == "lag"
        assert isinstance(expr.args[1], FieldRef)

    # --- _has_window ---

    def test_has_window_detects_nested(self):
        expr = parse_expression_str(
            'datetime_diff(lag(released_at).over(product_key, released_at), released_at, unit="days")'
        )
        assert _has_window(expr) is True

    def test_has_window_false_for_plain_expr(self):
        expr = parse_expression_str("sum(story_points)")
        assert _has_window(expr) is False

    # --- compile_expr in outer context ---

    def test_compile_window_in_outer_ctx(self):
        pv, pvle = self._pv_entity()
        outer_ctx = CompileContext(
            root_entity=pv,
            root_alias="base",
            entities={"ProductVersion": pv, "ProductVersionLifecycleEvent": pvle},
            join_steps=[],
            joined={},
            passthrough_cols={"product_version_key", "product_key", "released_at"},
        )
        expr = parse_expression_str("lag(released_at).over(product_key, released_at)")
        result = compile_expr(expr, outer_ctx)
        assert result == "LAG(base.released_at) OVER (PARTITION BY base.product_key ORDER BY base.released_at)"

    # --- compile_feature_to_sql (two-phase) ---

    def test_compile_window_feature_inner_outer_structure(self):
        feat = self._days_feat()
        released_at_feat = self._released_at_feat()
        entities = self._entities()
        features = {
            "product_version.released_at": released_at_feat,
            "product_version.days_since_prev_version": feat,
        }
        sql = compile_feature_to_sql(feat, entities, features=features)

        # Outer structure
        assert "SELECT base.product_version_key AS entity_id," in sql
        assert "AS value" in sql
        assert "FROM (" in sql
        assert ") base" in sql

        # Inner must contain the released_at expression and product_key
        assert "AS released_at" in sql
        assert "product_key" in sql
        assert "GROUP BY" in sql

        # Outer uses LAG window function
        assert "LAG(base.released_at) OVER (PARTITION BY base.product_key ORDER BY base.released_at)" in sql

    def test_compile_window_feature_sqlite_datetime_diff(self):
        feat = self._days_feat()
        released_at_feat = self._released_at_feat()
        entities = self._entities()
        features = {
            "product_version.released_at": released_at_feat,
            "product_version.days_since_prev_version": feat,
        }
        sql = compile_feature_to_sql(feat, entities, features=features)
        # SQLite datetime_diff uses julianday(substr(...))
        assert "julianday(substr(" in sql
        assert "CAST(" in sql

    def test_missing_feature_reference_raises(self):
        feat = self._days_feat()
        entities = self._entities()
        with pytest.raises(ValueError, match="references feature 'released_at'"):
            compile_feature_to_sql(feat, entities, features={feat.feature_id: feat})


# ---------------------------------------------------------------------------
# format_time function compilation
# ---------------------------------------------------------------------------

class TestFormatTime:
    def _ctx(self):
        entity = EntityDef(name="Commit", description="", identity=[], attributes=[], relations=[])
        return _make_ctx(entity, {"Commit": entity})

    def test_compile_via_expression_iso_week(self):
        """format_time in an expression compiles correctly via compile_expr."""
        from engine.expression_compiler import compile_expr
        from semantic.expression import parse_expression_str
        expr = parse_expression_str('format_time(committed_at, "YYYY-WW")')
        result = compile_expr(expr, self._ctx())
        assert "strftime('%G-%V'" in result
        assert "%W" not in result  # not Gregorian week

    def test_compile_via_expression_iso_month(self):
        from engine.expression_compiler import compile_expr
        from semantic.expression import parse_expression_str
        expr = parse_expression_str('format_time(merged_at, "YYYY-MM")')
        result = compile_expr(expr, self._ctx())
        assert "strftime('%G-%m'" in result

    def test_wrong_arg_count_raises(self):
        import pytest

        from engine.expression_compiler import compile_expr
        from semantic.expression import FieldRef, FuncCallExpr
        expr = FuncCallExpr(name="format_time", args=[FieldRef(["ts"])], kwargs={})
        with pytest.raises(ValueError, match="exactly 2"):
            compile_expr(expr, self._ctx())

    def test_non_literal_pattern_raises(self):
        import pytest

        from engine.expression_compiler import compile_expr
        from semantic.expression import FieldRef, FuncCallExpr
        expr = FuncCallExpr(
            name="format_time",
            args=[FieldRef(["ts"]), FieldRef(["pattern_col"])],
            kwargs={},
        )
        with pytest.raises(ValueError, match="string literal"):
            compile_expr(expr, self._ctx())


# ---------------------------------------------------------------------------
# json_has_key function compilation
# ---------------------------------------------------------------------------

class TestJsonHasKey:
    def _ctx(self):
        from semantic.ontology import EntityDef
        entity = EntityDef(
            name="TeamSprint",
            description="",
            identity={},
            attributes={},
            relations={},
        )
        return CompileContext(
            root_entity=entity,
            root_alias="ts",
            entities={"TeamSprint": entity},
            join_steps=[],
            joined={"TeamSprint": "ts"},
        )

    def test_parser_round_trip(self):
        expr = parse_expression_str("json_has_key(Sprint.issue_keys_added, issue_key)")
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "json_has_key"
        assert len(expr.args) == 2
        assert expr.args[0] == FieldRef(parts=["Sprint", "issue_keys_added"])
        assert expr.args[1] == FieldRef(parts=["issue_key"])

    def test_sqlite_rendering(self):
        expr = parse_expression_str("json_has_key(issue_keys_added, issue_key)")
        result = compile_expr(expr, self._ctx())
        assert result == "json_extract(ts.issue_keys_added, '$.' || ts.issue_key) IS NOT NULL"

    def test_wrong_arg_count_raises(self):
        expr = parse_expression_str("json_has_key(field)")
        with pytest.raises(ValueError, match="json_has_key requires exactly 2 arguments"):
            compile_expr(expr, self._ctx())

    def test_wrong_arg_count_three_raises(self):
        expr = parse_expression_str("json_has_key(a, b, c)")
        with pytest.raises(ValueError, match="json_has_key requires exactly 2 arguments"):
            compile_expr(expr, self._ctx())

    def test_in_case_when_sqlite(self):
        """Tests the typical usage pattern: CASE WHEN json_has_key(...) THEN ..."""
        expr = parse_expression_str(
            "sum(case when json_has_key(issue_keys_added, issue_key) then story_points end)"
        )
        result = compile_expr(expr, self._ctx())
        assert "json_extract(ts.issue_keys_added, '$.' || ts.issue_key) IS NOT NULL" in result
        assert result.startswith("SUM(CASE WHEN")


# ---------------------------------------------------------------------------
# compile_feature_to_sql — integration
# ---------------------------------------------------------------------------

class TestCompileFeatureToSql:
    def test_bare_field_no_join(self):
        entity = EntityDef(
            name="TeamSprint",
            identity=[AttributeDef("team_sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("score", ColumnType.FLOAT)],
        )
        feat = ExpressionFeatureDef(
            feature_id="team_sprint.score",
            name="Score",
            entity_type="TeamSprint",
            entity_key="team_sprint_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            expression=FieldRef(["score"]),
        )
        sql = compile_feature_to_sql(feat, {"TeamSprint": entity})
        assert "team_sprint_key AS entity_id" in sql
        assert "score AS value" in sql
        assert "team_sprint" in sql
        assert "JOIN" not in sql
        assert "GROUP BY" not in sql

    def test_aggregation_adds_group_by(self):
        entity = EntityDef(
            name="TeamSprint",
            identity=[AttributeDef("team_sprint_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("score", ColumnType.FLOAT)],
        )
        feat = ExpressionFeatureDef(
            feature_id="team_sprint.total",
            name="Total",
            entity_type="TeamSprint",
            entity_key="team_sprint_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            expression=AggExpr(func=AggFunc.SUM, expr=FieldRef(["score"])),
        )
        sql = compile_feature_to_sql(feat, {"TeamSprint": entity})
        assert "GROUP BY" in sql
        assert "team_sprint_key" in sql

    def test_traversal_join_emitted(self):
        sprint = _sprint_entity()
        ts = _team_sprint_entity()
        feat = ExpressionFeatureDef(
            feature_id="team_sprint.start_date",
            name="Start Date",
            entity_type="TeamSprint",
            entity_key="team_sprint_key",
            data_type="STRING",
            materialization_mode="materialized",
            expression=FieldRef(["Sprint", "start_date"]),
        )
        sql = compile_feature_to_sql(feat, {"TeamSprint": ts, "Sprint": sprint})
        assert "LEFT JOIN" in sql
        assert "sprint" in sql
        assert "sprint_key" in sql
        assert "start_date AS value" in sql

    def test_unknown_entity_raises(self):
        feat = ExpressionFeatureDef(
            feature_id="x.y",
            name="Y",
            entity_type="Unknown",
            entity_key="x_key",
            data_type="FLOAT",
            materialization_mode="materialized",
            expression=Lit(1),
        )
        with pytest.raises(ValueError, match="Unknown entity type"):
            compile_feature_to_sql(feat, {})


# ---------------------------------------------------------------------------
# argmax aggregate function
# ---------------------------------------------------------------------------

class TestArgmax:
    """Tests for argmax(value_field, key_field) compilation."""

    def _order_entities(self):
        """Simple Order → OrderItem (ONE_TO_MANY) fixture."""
        item = EntityDef(
            name="OrderItem",
            identity=[AttributeDef("order_item_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[
                AttributeDef("order_key", ColumnType.STRING, SemanticHint.RELATION),
                AttributeDef("name", ColumnType.STRING),
                AttributeDef("price", ColumnType.FLOAT),
            ],
        )
        order = EntityDef(
            name="Order",
            identity=[AttributeDef("order_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("customer_id", ColumnType.STRING)],
            relations=[
                RelationDef(
                    name="items",
                    target="OrderItem",
                    via=["order_key"],
                    cardinality=Cardinality.ONE_TO_MANY,
                )
            ],
        )
        return order, item

    def _product_entities(self):
        """Product → ProductVersion (inferred) → ProductVersionLifecycleEvent (explicit)."""
        pvle = EntityDef(
            name="ProductVersionLifecycleEvent",
            identity=[AttributeDef("lifecycle_event_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[
                AttributeDef("product_version_key", ColumnType.STRING, SemanticHint.RELATION),
                AttributeDef("to_lifecycle_state", ColumnType.STRING),
                AttributeDef("sequence", ColumnType.INTEGER),
            ],
            relations=[
                RelationDef(
                    name="product_version",
                    target="ProductVersion",
                    via="product_version_key",
                    cardinality=Cardinality.MANY_TO_ONE,
                )
            ],
        )
        pv = EntityDef(
            name="ProductVersion",
            identity=[AttributeDef("product_version_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("product_key", ColumnType.STRING, SemanticHint.RELATION)],
            relations=[
                RelationDef(
                    name="product",
                    target="Product",
                    via="product_key",
                    cardinality=Cardinality.MANY_TO_ONE,
                ),
                RelationDef(
                    name="lifecycle_events",
                    target="ProductVersionLifecycleEvent",
                    via=["product_version_key"],
                    cardinality=Cardinality.ONE_TO_MANY,
                ),
            ],
        )
        product = EntityDef(
            name="Product",
            identity=[AttributeDef("product_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("display_name", ColumnType.STRING)],
            # No explicit versions relation — inferred from ProductVersion.product MANY_TO_ONE
        )
        return product, pv, pvle

    # --- parser ---

    def test_argmax_parses_as_func_call(self):
        expr = parse_expression_str("argmax(name, price)")
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "argmax"
        assert len(expr.args) == 2
        assert expr.args[0] == FieldRef(parts=["name"])
        assert expr.args[1] == FieldRef(parts=["price"])

    def test_argmax_wrong_arg_count_raises(self):
        order, item = self._order_entities()
        ctx = _make_ctx(order, {"Order": order, "OrderItem": item})
        expr = FuncCallExpr(name="argmax", args=[FieldRef(["OrderItem", "name"])], kwargs={})
        with pytest.raises(ValueError, match="2 arguments"):
            compile_expr(expr, ctx)

    def test_argmax_key_not_fieldref_raises(self):
        order, item = self._order_entities()
        ctx = _make_ctx(order, {"Order": order, "OrderItem": item})
        expr = FuncCallExpr(
            name="argmax",
            args=[FieldRef(["OrderItem", "name"]), Lit(42)],
            kwargs={},
        )
        with pytest.raises(ValueError, match="key argument must be a field reference"):
            compile_expr(expr, ctx)

    def test_argmax_bare_root_field_key_raises(self):
        order, item = self._order_entities()
        ctx = _make_ctx(order, {"Order": order, "OrderItem": item})
        expr = FuncCallExpr(
            name="argmax",
            args=[FieldRef(["OrderItem", "name"]), FieldRef(["customer_id"])],
            kwargs={},
        )
        with pytest.raises(ValueError, match="ONE_TO_MANY"):
            compile_expr(expr, ctx)

    # --- _has_aggregation ---

    def test_argmax_is_aggregation(self):
        expr = parse_expression_str("argmax(name, price)")
        assert _has_aggregation(expr) is True

    def test_argmax_nested_in_case_when_is_aggregation(self):
        expr = parse_expression_str("case when argmax(name, price) = 'x' then 1 end")
        assert _has_aggregation(expr) is True

    # --- SQL compilation ---

    def test_argmax_compiles_max_case_when(self):
        order, item = self._order_entities()
        entities = {"Order": order, "OrderItem": item}
        ctx = _make_ctx(order, entities)
        expr = parse_expression_str("argmax(OrderItem.name, OrderItem.price)")
        result = compile_expr(expr, ctx)
        assert result.startswith("MAX(CASE WHEN")
        assert "__max_key" in result
        assert "THEN" in result

    def test_argmax_subquery_join_added(self):
        order, item = self._order_entities()
        entities = {"Order": order, "OrderItem": item}
        ctx = _make_ctx(order, entities)
        expr = parse_expression_str("argmax(OrderItem.name, OrderItem.price)")
        compile_expr(expr, ctx)
        subquery_steps = [s for s in ctx.join_steps if s.get("type") == "subquery"]
        assert len(subquery_steps) == 1
        step = subquery_steps[0]
        assert "MAX(price)" in step["subquery"]
        assert "order_item" in step["subquery"]
        assert "GROUP BY order_key" in step["subquery"]

    def test_argmax_compiles_feature_to_sql(self):
        """Full compile_feature_to_sql for an argmax feature."""
        order, item = self._order_entities()
        entities = {"Order": order, "OrderItem": item}
        feat = ExpressionFeatureDef(
            feature_id="order.most_expensive_item",
            name="Most Expensive Item",
            entity_type="Order",
            entity_key="order_key",
            data_type="STRING",
            materialization_mode="materialized",
            expression=parse_expression_str("argmax(OrderItem.name, OrderItem.price)"),
        )
        sql = compile_feature_to_sql(feat, entities)
        assert "order_key AS entity_id" in sql
        assert "MAX(CASE WHEN" in sql
        assert "__max_key" in sql
        assert "LEFT JOIN order_item" in sql
        assert "LEFT JOIN (SELECT order_key, MAX(price) AS __max_key" in sql
        assert "GROUP BY" in sql

    def test_argmax_multi_hop_product_lifecycle(self):
        """product.lifecycle_state pattern: Product → ProductVersion → PVLE (inferred + explicit)."""
        product, pv, pvle = self._product_entities()
        entities = {
            "Product": product,
            "ProductVersion": pv,
            "ProductVersionLifecycleEvent": pvle,
        }
        feat = ExpressionFeatureDef(
            feature_id="product.lifecycle_state",
            name="Lifecycle State",
            entity_type="Product",
            entity_key="product_key",
            data_type="STRING",
            materialization_mode="materialized",
            expression=parse_expression_str(
                "argmax(ProductVersion.ProductVersionLifecycleEvent.to_lifecycle_state,"
                "       ProductVersion.ProductVersionLifecycleEvent.sequence)"
            ),
        )
        sql = compile_feature_to_sql(feat, entities)

        # Structure
        assert "product_key AS entity_id" in sql
        assert "MAX(CASE WHEN" in sql
        assert "to_lifecycle_state" in sql
        assert "__max_key" in sql
        assert "GROUP BY" in sql

        # Both traversal joins present
        assert "LEFT JOIN product_version" in sql
        assert "LEFT JOIN product_version_lifecycle_event" in sql

        # Argmax subquery join
        assert "LEFT JOIN (SELECT product_version_key, MAX(sequence) AS __max_key" in sql
        assert "FROM product_version_lifecycle_event GROUP BY product_version_key" in sql


# ===========================================================================
# TestParserExtensions — Group E (ADR-121 Phase 1 regression)
# ===========================================================================

class TestParserExtensions:
    """Verify new AST nodes parse correctly and existing expressions are unchanged."""

    # --- Existing expression forms unchanged ---

    def test_sum_agg_still_parses(self):
        expr = parse_expression_str("sum(score)")
        assert isinstance(expr, AggExpr)
        assert expr.func == AggFunc.SUM

    def test_case_when_still_parses(self):
        expr = parse_expression_str("case when a > 0 then a else 0 end")
        assert isinstance(expr, CaseWhenExpr)

    def test_lag_over_still_parses_as_window(self):
        """lag(price).over(product_key, released_at) must still be WindowExpr."""
        expr = parse_expression_str("lag(price).over(product_key, released_at)")
        assert isinstance(expr, WindowExpr)
        assert expr.func == "lag"

    def test_arithmetic_unchanged(self):
        expr = parse_expression_str("(a + b) / c")
        assert isinstance(expr, BinOp)
        assert expr.op == "/"

    def test_datetime_diff_still_parses(self):
        expr = parse_expression_str(
            'datetime_diff(created_at, merged_at, unit="hours")'
        )
        assert isinstance(expr, FuncCallExpr)
        assert expr.name == "datetime_diff"

    # --- New construct parsing ---

    def test_list_lit(self):
        expr = parse_expression_str('["a", "b"]')
        assert isinstance(expr, ListLit)
        assert len(expr.values) == 2
        assert expr.values[0] == Lit("a")

    def test_in_expr(self):
        expr = parse_expression_str('x in ["a", "b"]')
        assert isinstance(expr, InExpr)
        assert not expr.negated
        assert len(expr.values) == 2

    def test_not_in_expr(self):
        expr = parse_expression_str('x not_in ["c"]')
        assert isinstance(expr, InExpr)
        assert expr.negated
        assert len(expr.values) == 1

    def test_duration_lit_days(self):
        expr = parse_expression_str("7d")
        assert isinstance(expr, DurationLit)
        assert expr.amount == 7
        assert expr.unit == "d"

    def test_duration_lit_weeks(self):
        expr = parse_expression_str("4w")
        assert isinstance(expr, DurationLit)
        assert expr.amount == 4
        assert expr.unit == "w"

    def test_method_call_startswith(self):
        expr = parse_expression_str('field.startswith("x")')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "startswith"
        assert isinstance(expr.expr, FieldRef)

    def test_method_call_is_null(self):
        expr = parse_expression_str("field.is_null()")
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "is_null"
        assert expr.args == []

    def test_method_call_not_null(self):
        expr = parse_expression_str("field.not_null()")
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "not_null"

    def test_method_call_contains_any_with_list(self):
        expr = parse_expression_str('labels.containsAny(["bug", "defect"])')
        assert isinstance(expr, MethodCallExpr)
        assert expr.method == "containsAny"
        assert isinstance(expr.args[0], ListLit)

    def test_lag_over_not_method_call(self):
        """Confirm .over() is still parsed as WindowExpr, NOT MethodCallExpr."""
        expr = parse_expression_str("lag(price).over(product_key, released_at)")
        assert isinstance(expr, WindowExpr)
        assert not isinstance(expr, MethodCallExpr)

    def test_traversal_field_ref(self):
        expr = parse_expression_str("Repository.Team.display_name")
        assert isinstance(expr, FieldRef)
        assert expr.parts == ["Repository", "Team", "display_name"]

    def test_now_minus_duration(self):
        expr = parse_expression_str("now() - 7d")
        assert isinstance(expr, BinOp)
        assert expr.op == "-"
        assert isinstance(expr.right, DurationLit)
