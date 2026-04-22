# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic mapper subsystem — transforms, parsing, validation, engine."""

from pathlib import Path

import pytest
import yaml
from core.validation import validate_schema
from engine.filter_compiler import (
    FilterCompileContext,
    collect_filter_column_refs,
    compile_filter_expr,
    compile_filter_yaml,
)
from engine.mapper import (
    FanOutError,
    MapperError,
    _build_source_query,
    _detect_fan_out,
    _resolve_write_context,
    run_mapper,
    topological_sort,
)
from engine.python_evaluator import (
    collect_enum_refs,
    collect_refs,
    evaluate,
)
from semantic.expression import (
    BinOp,
    FieldRef,
    FuncCallExpr,
    InExpr,
    Lit,
    MethodCallExpr,
    parse_expression_str,
)
from semantic.mapper import (
    _check_mapper_rules,
    load_all_mappers,
    load_mapper_by_name,
    parse_mapper_yaml,
    validate_mapper_yaml,
)
from semantic.mapper_types import EnumMapping, MapperConfig
from semantic.types import (
    AttributeDef,
    ColumnType,
    EntityDef,
    EventDef,
    HistoryDef,
    HistoryType,
    SemanticHint,
)

MAPPERS_DIR = Path(__file__).resolve().parent.parent / "models" / "example" / "mappers"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(tmp_path, data, filename="test_mapper.yaml"):
    p = tmp_path / filename
    p.write_text(yaml.dump(data, sort_keys=False))
    return p


def _minimal_mapper(**overrides):
    """Return a minimal valid mapper dict."""
    obj = {
        "mapper": "test_mapper",
        "grain": "src",
        "sources": [
            {"alias": "src", "table": "raw_table", "columns": ["product_key", "display_name"]},
        ],
        "targets": [
            {
                "entity": "Product",
                "columns": [
                    {"name": "product_key", "expr": "src.product_key"},
                    {"name": "display_name", "expr": "src.display_name"},
                ],
            }
        ],
    }
    obj.update(overrides)
    return obj


def _minimal_fact_mapper(**overrides):
    """Return a minimal valid event (DEDUP) mapper dict."""
    obj = {
        "mapper": "test_fact_mapper",
        "grain": "src",
        "sources": [
            {"alias": "src", "table": "raw_events", "columns": ["test_key", "event_id", "value"]},
        ],
        "targets": [
            {
                "entity": "TestFactEvent",
                "columns": [
                    {"name": "test_key", "expr": "src.test_key"},
                    {"name": "event_id", "expr": "src.event_id"},
                    {"name": "value", "expr": "src.value"},
                ],
            }
        ],
    }
    obj.update(overrides)
    return obj


def _make_product_ontology():
    return EntityDef(
        name="Product",
        identity=[
            AttributeDef("product_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
        ],
        attributes=[
            AttributeDef("display_name", ColumnType.STRING, SemanticHint.IDENTIFIER),
        ],
        history=HistoryDef(
            history_type=HistoryType.SCD2,
            columns=[
                AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
            ],
        ),
    )


def _make_fact_ontology():
    """Return a parent entity with an event sub-entity for DEDUP testing."""
    return EntityDef(
        name="Test",
        identity=[
            AttributeDef("test_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
        ],
        events=[
            EventDef(
                name="fact",
                attributes=[
                    AttributeDef("event_id", ColumnType.STRING),
                    AttributeDef("value", ColumnType.STRING),
                    AttributeDef("inserted_at", ColumnType.STRING, SemanticHint.INSERTED_AT),
                ],
            ),
        ],
    )


def _make_product_no_history_ontology():
    return EntityDef(
        name="Product",
        identity=[
            AttributeDef("product_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
        ],
        attributes=[
            AttributeDef("display_name", ColumnType.STRING, SemanticHint.IDENTIFIER),
        ],
    )


def _setup_db(ch_backend, entities):
    from semantic.materialize import materialize_with_backend
    materialize_with_backend(entities, ch_backend)
    return ch_backend


# ===========================================================================
# 1. Transform parsing
# ===========================================================================

class TestTransformParsing:
    def test_bare_col_ref(self):
        node = parse_expression_str("src.name")
        assert isinstance(node, FieldRef)
        assert node.parts == ["src", "name"]

    def test_single_function(self):
        node = parse_expression_str("TRIM(src.name)")
        assert isinstance(node, FuncCallExpr)
        assert node.name == "trim"  # new parser lowercases function names
        assert len(node.args) == 1
        assert isinstance(node.args[0], FieldRef)

    def test_nested_call(self):
        node = parse_expression_str("UPPER_SNAKE(TRIM(src.name))")
        assert isinstance(node, FuncCallExpr)
        assert node.name == "upper_snake"  # new parser lowercases function names
        inner = node.args[0]
        assert isinstance(inner, FuncCallExpr)
        assert inner.name == "trim"

    def test_cast_with_type(self):
        node = parse_expression_str("CAST(src.id, integer)")
        assert isinstance(node, FuncCallExpr)
        assert node.name == "cast"  # new parser lowercases function names
        # In new AST bare identifiers become FieldRef with 1 part
        assert isinstance(node.args[1], FieldRef)
        assert node.args[1].parts == ["integer"]

    def test_default_with_string(self):
        node = parse_expression_str("DEFAULT(src.name, 'unknown')")
        assert isinstance(node, FuncCallExpr)
        assert node.name == "default"  # new parser lowercases function names
        assert isinstance(node.args[1], Lit)
        assert node.args[1].value == "unknown"

    def test_enum_args(self):
        node = parse_expression_str("ENUM(src.status, lifecycle_state)")
        assert isinstance(node, FuncCallExpr)
        assert node.name == "enum"  # new parser lowercases function names
        # mapping_name is a bare FieldRef with 1 part
        assert isinstance(node.args[1], FieldRef)
        assert node.args[1].parts == ["lifecycle_state"]

    def test_parse_error_empty(self):
        with pytest.raises((ValueError, Exception)):
            parse_expression_str("")

    def test_parse_error_bad_syntax(self):
        with pytest.raises((ValueError, Exception)):
            parse_expression_str("@@bad")

    def test_parse_error_trailing(self):
        with pytest.raises((ValueError, Exception)):
            parse_expression_str("src.name extra")


# ===========================================================================
# 2. Transform evaluation
# ===========================================================================

class TestTransformEvaluation:
    ROW = {"src": {"name": "  Hello World  ", "id": "42", "status": "Active"}}

    def test_trim(self):
        node = parse_expression_str("TRIM(src.name)")
        assert evaluate(node, self.ROW) == "Hello World"

    def test_upper_snake(self):
        node = parse_expression_str("UPPER_SNAKE(TRIM(src.name))")
        assert evaluate(node, self.ROW) == "HELLO_WORLD"

    def test_title(self):
        node = parse_expression_str("TITLE(src.name)")
        assert evaluate(node, self.ROW) == "  Hello World  "

    def test_lower(self):
        node = parse_expression_str("LOWER(TRIM(src.name))")
        assert evaluate(node, self.ROW) == "hello world"

    def test_cast_integer(self):
        node = parse_expression_str("CAST(src.id, integer)")
        assert evaluate(node, self.ROW) == 42

    def test_cast_float(self):
        node = parse_expression_str("CAST(src.id, float)")
        assert evaluate(node, self.ROW) == 42.0

    def test_cast_boolean(self):
        row = {"src": {"val": "true"}}
        node = parse_expression_str("CAST(src.val, boolean)")
        assert evaluate(node, row) is True

    def test_cast_string(self):
        node = parse_expression_str("CAST(src.id, string)")
        assert evaluate(node, self.ROW) == "42"

    def test_default_non_null(self):
        node = parse_expression_str("DEFAULT(src.name, 'fallback')")
        assert evaluate(node, self.ROW) == "  Hello World  "

    def test_default_null(self):
        row = {"src": {"name": None}}
        node = parse_expression_str("DEFAULT(src.name, 'fallback')")
        assert evaluate(node, row) == "fallback"

    def test_enum_lookup(self):
        enum = EnumMapping("lifecycle_state", {"ACTIVE": ["Active", "active"]})
        enums = {"lifecycle_state": enum}
        node = parse_expression_str("ENUM(src.status, lifecycle_state)")
        assert evaluate(node, self.ROW, enums) == "ACTIVE"

    def test_enum_not_found(self):
        enum = EnumMapping("lifecycle_state", {"ACTIVE": ["active"]})
        enums = {"lifecycle_state": enum}
        row = {"src": {"status": "UNKNOWN"}}
        node = parse_expression_str("ENUM(src.status, lifecycle_state)")
        with pytest.raises(ValueError, match="No enum mapping"):
            evaluate(node, row, enums)

    def test_null_propagation_trim(self):
        row = {"src": {"name": None}}
        node = parse_expression_str("TRIM(src.name)")
        assert evaluate(node, row) is None

    def test_null_propagation_upper_snake(self):
        row = {"src": {"name": None}}
        node = parse_expression_str("UPPER_SNAKE(src.name)")
        assert evaluate(node, row) is None

    def test_null_propagation_cast(self):
        row = {"src": {"id": None}}
        node = parse_expression_str("CAST(src.id, integer)")
        assert evaluate(node, row) is None

    def test_enum_null_propagation(self):
        enum = EnumMapping("lifecycle_state", {"ACTIVE": ["Active"]})
        enums = {"lifecycle_state": enum}
        row = {"src": {"status": None}}
        node = parse_expression_str("ENUM(src.status, lifecycle_state)")
        assert evaluate(node, row, enums) is None

    def test_col_ref_missing_alias(self):
        row = {"other": {"name": "x"}}
        node = parse_expression_str("src.name")
        assert evaluate(node, row) is None


# ===========================================================================
# 3. Collect refs
# ===========================================================================

class TestCollectRefs:
    def test_bare_ref(self):
        node = parse_expression_str("src.name")
        refs = collect_refs(node)
        assert len(refs) == 1
        assert refs[0].parts[0] == "src"
        assert refs[0].parts[1] == "name"

    def test_nested_refs(self):
        node = parse_expression_str("TRIM(src.name)")
        refs = collect_refs(node)
        assert len(refs) == 1

    def test_multi_arg_refs(self):
        node = parse_expression_str("DEFAULT(src.name, 'x')")
        refs = collect_refs(node)
        assert len(refs) == 1

    def test_enum_refs(self):
        node = parse_expression_str("ENUM(src.status, lifecycle_state)")
        names = collect_enum_refs(node)
        assert "lifecycle_state" in names

    def test_no_enum_refs(self):
        node = parse_expression_str("TRIM(src.name)")
        assert collect_enum_refs(node) == []


# ===========================================================================
# 3a. Filter expression parsing
# ===========================================================================

class TestFilterParsing:
    def test_equality(self):
        node = parse_expression_str('issue_type == "Product Version"')
        assert isinstance(node, BinOp)
        # new parser normalizes == to = for SQL compatibility
        assert node.op in ("==", "=")

    def test_equality_single_equals(self):
        # bare = should be accepted as a synonym for ==
        node = parse_expression_str("status = 'open'")
        assert isinstance(node, BinOp)
        assert node.op == "="
        assert node.left.parts == ["status"]
        assert node.right.value == "open"

    def test_equality_single_equals_traversal(self):
        # bare = on a traversal ref (e.g. Repository.Team.display_name = 'X')
        node = parse_expression_str("Repository.Team.display_name = 'Lovelace'")
        assert isinstance(node, BinOp)
        assert node.op == "="
        assert isinstance(node.left, FieldRef)
        assert node.left.parts == ["Repository", "Team", "display_name"]
        assert node.right.value == "Lovelace"

    def test_inequality(self):
        node = parse_expression_str('status != "closed"')
        assert isinstance(node, BinOp)
        assert node.op == "!="

    def test_greater_than(self):
        node = parse_expression_str("priority > 3")
        assert isinstance(node, BinOp)
        assert node.op == ">"
        assert isinstance(node.right, Lit)
        assert node.right.value == 3

    def test_less_than(self):
        node = parse_expression_str("priority < 10")
        assert isinstance(node, BinOp)
        assert node.op == "<"

    def test_greater_equal(self):
        node = parse_expression_str("priority >= 5")
        assert isinstance(node, BinOp)
        assert node.op == ">="

    def test_less_equal(self):
        node = parse_expression_str("priority <= 5")
        assert isinstance(node, BinOp)
        assert node.op == "<="

    def test_float_number(self):
        node = parse_expression_str("score > 3.5")
        assert isinstance(node, BinOp)
        assert isinstance(node.right, Lit)
        assert node.right.value == 3.5

    def test_in_list(self):
        node = parse_expression_str('status in ["open", "active"]')
        assert isinstance(node, InExpr)
        assert isinstance(node.expr, FieldRef)
        assert node.expr.parts == ["status"]
        assert not node.negated
        assert len(node.values) == 2
        assert node.values[0].value == "open"

    def test_not_in_list(self):
        node = parse_expression_str('status not_in ["closed", "archived"]')
        assert isinstance(node, InExpr)
        assert node.negated

    def test_method_contains(self):
        node = parse_expression_str('labels.contains("critical")')
        assert isinstance(node, MethodCallExpr)
        assert isinstance(node.expr, FieldRef)
        assert node.expr.parts == ["labels"]
        assert node.method == "contains"
        assert len(node.args) == 1
        assert isinstance(node.args[0], Lit)

    def test_method_contains_any(self):
        node = parse_expression_str('labels.containsAny(["bug", "defect"])')
        assert isinstance(node, MethodCallExpr)
        assert node.method == "containsAny"
        # In new AST, list args are ListLit nodes
        from semantic.expression import ListLit
        assert isinstance(node.args[0], ListLit)
        assert len(node.args[0].values) == 2

    def test_method_startswith(self):
        node = parse_expression_str('key.startswith("AD-")')
        assert isinstance(node, MethodCallExpr)
        assert node.method == "startswith"

    def test_method_endswith(self):
        node = parse_expression_str('name.endswith("Review")')
        assert isinstance(node, MethodCallExpr)
        assert node.method == "endswith"

    def test_method_is_null(self):
        node = parse_expression_str("resolved_date.is_null()")
        assert isinstance(node, MethodCallExpr)
        assert node.method == "is_null"
        assert node.args == []

    def test_method_not_null(self):
        node = parse_expression_str("assigned_to.not_null()")
        assert isinstance(node, MethodCallExpr)
        assert node.method == "not_null"

    def test_parse_error_empty(self):
        with pytest.raises((ValueError, Exception)):
            parse_expression_str("")

    def test_parse_error_bad_syntax(self):
        with pytest.raises((ValueError, Exception)):
            parse_expression_str("@@bad")

    def test_parse_error_incomplete(self):
        with pytest.raises((ValueError, Exception)):
            parse_expression_str("status ==")


# ===========================================================================
# 3b. Filter SQL compilation
# ===========================================================================

class TestFilterCompilation:
    def _compile(self, expr_str: str, alias=None) -> str:
        """Helper: parse + compile a filter expression string to SQL."""
        node = parse_expression_str(expr_str)
        return compile_filter_expr(node, FilterCompileContext(alias=alias))

    def test_equality_sql(self):
        sql = self._compile('issue_type == "Story"', alias="src")
        assert sql == "src.issue_type = 'Story'"

    def test_equality_no_alias(self):
        sql = self._compile('issue_type == "Story"', alias=None)
        assert sql == "issue_type = 'Story'"

    def test_inequality_sql(self):
        sql = self._compile('status != "closed"', alias="t")
        assert sql == "t.status != 'closed'"

    def test_greater_than_number(self):
        sql = self._compile("priority > 3")
        assert sql == "priority > 3"

    def test_in_sql(self):
        sql = self._compile('status in ["open", "active"]', alias="s")
        assert sql == "s.status IN ('open', 'active')"

    def test_not_in_sql(self):
        sql = self._compile('status not_in ["closed"]')
        assert sql == "status NOT IN ('closed')"

    def test_contains_json_each(self):
        sql = self._compile('labels.contains("bug")', alias="s")
        assert "json_each(s.labels)" in sql
        assert "value = 'bug'" in sql

    def test_contains_any_json_each(self):
        sql = self._compile('labels.containsAny(["bug", "defect"])', alias="s")
        assert "json_each(s.labels)" in sql
        assert "value IN ('bug', 'defect')" in sql

    def test_startswith_like(self):
        sql = self._compile('key.startswith("AD-")')
        assert sql == "key LIKE 'AD-%' ESCAPE '\\'"

    def test_endswith_like(self):
        sql = self._compile('name.endswith("Review")')
        assert sql == "name LIKE '%Review' ESCAPE '\\'"

    def test_is_null_sql(self):
        sql = self._compile("col.is_null()", alias="t")
        assert sql == "t.col IS NULL"

    def test_not_null_sql(self):
        sql = self._compile("col.not_null()")
        assert sql == "col IS NOT NULL"

    def test_and_composition(self):
        filt = {"and": ['status == "open"', "priority > 3"]}
        sql = compile_filter_yaml(filt)
        assert sql == "(status = 'open' AND priority > 3)"

    def test_or_composition(self):
        filt = {"or": ['status == "open"', 'status == "active"']}
        sql = compile_filter_yaml(filt)
        assert sql == "(status = 'open' OR status = 'active')"

    def test_not_composition(self):
        filt = {"not": 'status == "closed"'}
        sql = compile_filter_yaml(filt)
        assert sql == "NOT (status = 'closed')"

    def test_nested_composition(self):
        filt = {
            "and": [
                'project_type == "software"',
                {"or": ['status == "open"', 'status == "active"']},
            ]
        }
        sql = compile_filter_yaml(filt)
        assert sql == "(project_type = 'software' AND (status = 'open' OR status = 'active'))"

    def test_string_leaf(self):
        sql = compile_filter_yaml('project_type == "software"')
        assert sql == "project_type = 'software'"

    def test_collect_filter_column_refs_simple(self):
        refs = collect_filter_column_refs('issue_type == "Story"')
        assert refs == ["issue_type"]

    def test_collect_filter_column_refs_composed(self):
        filt = {"and": ['status == "open"', 'priority > 3']}
        refs = collect_filter_column_refs(filt)
        assert set(refs) == {"status", "priority"}

    def test_collect_filter_column_refs_method(self):
        refs = collect_filter_column_refs('labels.contains("bug")')
        assert refs == ["labels"]

    def test_startswith_escapes_wildcards(self):
        sql = self._compile('key.startswith("50%")')
        assert sql == "key LIKE '50\\%%' ESCAPE '\\'"


# ===========================================================================
# 4. Schema validation
# ===========================================================================

class TestSchemaValidation:
    def test_valid_mapper(self):
        assert validate_schema(_minimal_mapper(), "mapper") == []

    def test_valid_fact_mapper(self):
        assert validate_schema(_minimal_fact_mapper(), "mapper") == []

    def test_missing_mapper_name(self):
        data = _minimal_mapper()
        del data["mapper"]
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1
        assert any("mapper" in e.message for e in errs)

    def test_missing_grain(self):
        data = _minimal_mapper()
        del data["grain"]
        errs = validate_schema(data, "mapper")
        assert errs == []  # grain is optional in schema; enforced semantically

    def test_missing_sources(self):
        data = _minimal_mapper()
        del data["sources"]
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1

    def test_missing_targets(self):
        data = _minimal_mapper()
        del data["targets"]
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1

    def test_invalid_mapper_name_pattern(self):
        data = _minimal_mapper(mapper="BadName")
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1

    def test_invalid_target_entity_pattern(self):
        data = _minimal_mapper()
        data["targets"][0]["entity"] = "bad_name"
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1

    def test_invalid_join_type_enum(self):
        data = _minimal_mapper()
        data["joins"] = [{
            "from": "src",
            "to": "other",
            "type": "outer",
            "on": [{"left": "src.product_key", "right": "other.oid"}],
        }]
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1

    def test_additional_property_at_root(self):
        data = _minimal_mapper()
        data["extra"] = "bad"
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1

    def test_valid_filter_string(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = 'display_name == "test"'
        assert validate_schema(data, "mapper") == []

    def test_valid_filter_and(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = {"and": ['display_name == "test"', 'product_key > 0']}
        assert validate_schema(data, "mapper") == []

    def test_valid_filter_or(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = {"or": ['display_name == "a"', 'display_name == "b"']}
        assert validate_schema(data, "mapper") == []

    def test_valid_filter_not(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = {"not": 'display_name == "test"'}
        assert validate_schema(data, "mapper") == []

    def test_invalid_filter_type(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = 42
        errs = validate_schema(data, "mapper")
        assert len(errs) >= 1


# ===========================================================================
# 5. Semantic validation
# ===========================================================================

class TestSemanticValidation:
    def test_valid_passes(self):
        assert _check_mapper_rules(_minimal_mapper()) == []

    def test_grain_required_when_no_rows(self):
        data = _minimal_mapper()
        del data["grain"]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "grain_required" for e in errs)

    def test_grain_not_required_with_rows(self):
        data = {
            "mapper": "test_multi",
            "sources": [
                {"alias": "src", "table": "raw_table", "columns": ["product_key", "display_name"]},
            ],
            "targets": [{
                "entity": "Product",
                "rows": [{
                    "from": "src",
                    "columns": [
                        {"name": "product_key", "expr": "src.product_key"},
                        {"name": "display_name", "expr": "src.display_name"},
                    ],
                }],
            }],
        }
        errs = _check_mapper_rules(data)
        assert not any(e.rule == "grain_required" for e in errs)

    def test_target_rows_or_columns_both(self):
        data = _minimal_mapper()
        data["targets"][0]["rows"] = [{
            "from": "src",
            "columns": [{"name": "product_key", "expr": "src.product_key"}],
        }]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "target_rows_or_columns" for e in errs)

    def test_target_rows_or_columns_neither(self):
        data = _minimal_mapper()
        data["targets"][0].pop("columns")
        errs = _check_mapper_rules(data)
        assert any(e.rule == "target_rows_or_columns" for e in errs)

    def test_row_group_alias_exists(self):
        data = {
            "mapper": "test_multi",
            "sources": [
                {"alias": "src", "table": "raw_table", "columns": ["product_key"]},
            ],
            "targets": [{
                "entity": "Product",
                "rows": [{
                    "from": "missing",
                    "columns": [{"name": "product_key", "expr": "src.product_key"}],
                }],
            }],
        }
        errs = _check_mapper_rules(data)
        assert any(e.rule == "row_group_alias_exists" for e in errs)

    def test_grain_alias_exists(self):
        data = _minimal_mapper(grain="nonexistent")
        errs = _check_mapper_rules(data)
        assert any(e.rule == "grain_alias_exists" for e in errs)

    def test_unique_source_alias(self):
        data = _minimal_mapper()
        data["sources"].append({"alias": "src", "table": "other", "columns": ["x"]})
        errs = _check_mapper_rules(data)
        assert any(e.rule == "unique_source_alias" for e in errs)

    def test_join_alias_exists(self):
        data = _minimal_mapper()
        data["joins"] = [{
            "from": "src",
            "to": "missing",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "missing.product_key"}],
        }]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "join_alias_exists" for e in errs)

    def test_join_column_ref_format(self):
        data = _minimal_mapper()
        data["sources"].append({"alias": "other", "table": "other_t", "columns": ["oid"]})
        data["joins"] = [{
            "from": "src",
            "to": "other",
            "type": "left",
            "on": [{"left": "bad_ref", "right": "other.oid"}],
        }]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "join_column_ref_format" for e in errs)

    def test_join_column_ref(self):
        data = _minimal_mapper()
        data["sources"].append({"alias": "other", "table": "other_t", "columns": ["oid"]})
        data["joins"] = [{
            "from": "src",
            "to": "other",
            "type": "left",
            "on": [{"left": "src.nonexistent", "right": "other.oid"}],
        }]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "join_column_ref" for e in errs)

    def test_json_array_contains_requires_json_path(self):
        data = _minimal_mapper()
        data["sources"].append({"alias": "ref", "table": "ref_t", "columns": ["aliases"]})
        data["joins"] = [{
            "from": "src",
            "to": "ref",
            "type": "left",
            "on": [{
                "left": "src.product_key",
                "right": "ref.aliases",
                "operator": "json_array_contains",
            }],
        }]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "json_array_contains_requires_path" for e in errs)

    def test_json_path_requires_operator(self):
        data = _minimal_mapper()
        data["sources"].append({"alias": "ref", "table": "ref_t", "columns": ["aliases"]})
        data["joins"] = [{
            "from": "src",
            "to": "ref",
            "type": "left",
            "on": [{
                "left": "src.product_key",
                "right": "ref.aliases",
                "json_path": "$.jira",
            }],
        }]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "json_path_requires_operator" for e in errs)

    def test_json_array_contains_valid(self):
        data = _minimal_mapper()
        data["sources"].append({"alias": "ref", "table": "ref_t", "columns": ["aliases"]})
        data["joins"] = [{
            "from": "src",
            "to": "ref",
            "type": "left",
            "on": [{
                "left": "src.product_key",
                "right": "ref.aliases",
                "operator": "json_array_contains",
                "json_path": "$.jira",
            }],
        }]
        errs = _check_mapper_rules(data)
        assert not any(e.rule in ("json_array_contains_requires_path",
                                   "json_path_requires_operator") for e in errs)

    def test_expression_parse_error(self):
        data = _minimal_mapper()
        data["targets"][0]["columns"][0]["expr"] = "@@bad"
        errs = _check_mapper_rules(data)
        assert any(e.rule == "expression_parse" for e in errs)

    def test_expression_alias_ref(self):
        data = _minimal_mapper()
        data["targets"][0]["columns"][0]["expr"] = "missing.id"
        errs = _check_mapper_rules(data)
        assert any(e.rule == "expression_alias_ref" for e in errs)

    def test_expression_column_ref(self):
        data = _minimal_mapper()
        data["targets"][0]["columns"][0]["expr"] = "src.nonexistent"
        errs = _check_mapper_rules(data)
        assert any(e.rule == "expression_column_ref" for e in errs)

    def test_enum_mapping_ref(self):
        data = _minimal_mapper()
        data["targets"][0]["columns"][0]["expr"] = "enum(src.display_name, missing_enum)"
        errs = _check_mapper_rules(data)
        assert any(e.rule == "enum_mapping_ref" for e in errs)

    def test_filter_parse_error(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = "@@bad expression"
        errs = _check_mapper_rules(data)
        assert any(e.rule == "filter_parse" for e in errs)

    def test_filter_column_ref_missing(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = 'nonexistent == "test"'
        errs = _check_mapper_rules(data)
        assert any(e.rule == "filter_column_ref" for e in errs)

    def test_filter_column_ref_valid(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = 'display_name == "test"'
        errs = _check_mapper_rules(data)
        assert not any(e.rule == "filter_column_ref" for e in errs)

    def test_filter_composed_column_ref(self):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = {
            "and": ['display_name == "test"', 'nonexistent > 5']
        }
        errs = _check_mapper_rules(data)
        assert any(e.rule == "filter_column_ref" for e in errs)

    def test_deduplicate_column_ref_valid(self):
        data = _minimal_mapper()
        data["sources"][0]["deduplicate"] = ["product_key"]
        errs = _check_mapper_rules(data)
        assert not any(e.rule == "deduplicate_column_ref" for e in errs)

    def test_deduplicate_column_ref_missing(self):
        data = _minimal_mapper()
        data["sources"][0]["deduplicate"] = ["nonexistent"]
        errs = _check_mapper_rules(data)
        assert any(e.rule == "deduplicate_column_ref" for e in errs)


# ===========================================================================
# 6. Query building
# ===========================================================================

class TestQueryBuilding:
    def test_single_source(self):
        config = parse_mapper_yaml(_write_yaml(
            Path("/tmp"), _minimal_mapper(), "qb1.yaml"
        ))
        sql = _build_source_query(config)
        assert "FROM raw_table AS src" in sql
        assert '"src.product_key"' in sql
        assert '"src.display_name"' in sql

    def test_left_join(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "other", "table": "other_table", "columns": ["oid", "label"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "other",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "other.oid"}],
        }]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "LEFT JOIN other_table AS other" in sql
        assert "src.product_key = other.oid" in sql

    def test_inner_join(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "other", "table": "other_table", "columns": ["oid"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "other",
            "type": "inner",
            "on": [{"left": "src.product_key", "right": "other.oid"}],
        }]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "INNER JOIN other_table AS other" in sql

    def test_multiple_joins(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].extend([
            {"alias": "a", "table": "table_a", "columns": ["aid"]},
            {"alias": "b", "table": "table_b", "columns": ["bid"]},
        ])
        data["joins"] = [
            {"from": "src", "to": "a", "type": "left",
             "on": [{"left": "src.product_key", "right": "a.aid"}]},
            {"from": "src", "to": "b", "type": "inner",
             "on": [{"left": "src.product_key", "right": "b.bid"}]},
        ]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "LEFT JOIN table_a" in sql
        assert "INNER JOIN table_b" in sql

    def test_source_with_filter_subquery(self, tmp_path):
        data = _minimal_mapper()
        data["sources"][0]["filter"] = 'display_name == "test"'
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "(SELECT * FROM raw_table WHERE display_name = 'test') AS src" in sql

    def test_joined_source_with_filter(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "det", "table": "raw_details", "columns": ["did", "label"],
             "filter": 'label == "important"'}
        )
        data["joins"] = [{
            "from": "src",
            "to": "det",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "det.did"}],
        }]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "LEFT JOIN (SELECT * FROM raw_details WHERE label = 'important') AS det" in sql

    def test_no_filter_no_subquery(self, tmp_path):
        data = _minimal_mapper()
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "FROM raw_table AS src" in sql
        assert "SELECT * FROM" not in sql

    def test_deduplicate_generates_group_by(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "det", "table": "raw_details", "columns": ["did", "label"],
             "deduplicate": ["did"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "det",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "det.did"}],
        }]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "(SELECT * FROM raw_details GROUP BY did) AS det" in sql

    def test_deduplicate_with_filter(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "det", "table": "raw_details", "columns": ["did", "label"],
             "filter": 'label == "important"',
             "deduplicate": ["did"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "det",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "det.did"}],
        }]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "(SELECT * FROM raw_details WHERE label = 'important' GROUP BY did) AS det" in sql

    def test_json_array_contains_join(self, tmp_path):
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "ref", "table": "ref_table", "columns": ["rkey", "aliases"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "ref",
            "type": "left",
            "on": [{
                "left": "src.display_name",
                "right": "ref.aliases",
                "operator": "json_array_contains",
                "json_path": "$.jira",
            }],
        }]
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "LEFT JOIN ref_table AS ref" in sql
        assert "src.display_name IN (SELECT value FROM json_each(json_extract(ref.aliases, '$.jira')))" in sql

    def test_json_array_contains_without_json_path(self, tmp_path):
        """When json_path is omitted (plain array column), no json_extract wrapper."""
        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "ref", "table": "ref_table", "columns": ["rkey", "tags"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "ref",
            "type": "left",
            "on": [{
                "left": "src.display_name",
                "right": "ref.tags",
                "operator": "json_array_contains",
            }],
        }]
        # Schema validation requires json_path for json_array_contains,
        # but we test the SQL compilation directly here
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        sql = _build_source_query(config)
        assert "src.display_name IN (SELECT value FROM json_each(ref.tags))" in sql


# ===========================================================================
# 7. Fan-out detection
# ===========================================================================

class TestFanOutDetection:
    def test_no_fan_out(self):
        rows = [
            {"src.id": 1, "src.name": "a"},
            {"src.id": 2, "src.name": "b"},
        ]
        assert _detect_fan_out(rows, "src", ["id"]) is None

    def test_fan_out_detected(self):
        rows = [
            {"src.id": 1, "src.name": "a"},
            {"src.id": 1, "src.name": "b"},
        ]
        result = _detect_fan_out(rows, "src", ["id"])
        assert result is not None
        assert "Fan-out" in result


# ===========================================================================
# 8. Execution
# ===========================================================================

class TestExecution:
    def _create_raw_table(self, backend, table_name, columns, rows, primary_keys=None):
        """Create a raw source table and insert data.

        By default only the first column is used as ORDER BY so non-key columns
        remain Nullable(String), allowing LEFT JOINs to return NULL for
        unmatched rows.  Pass *primary_keys* explicitly when all rows must
        survive FINAL (e.g. event/fact tables with a composite natural key).
        """
        pks = primary_keys if primary_keys is not None else [columns[0]]
        backend.create_table(table_name, {c: "String" for c in columns}, primary_keys=pks)
        if rows:
            backend.bulk_upsert(table_name, [dict(zip(columns, r)) for r in rows])

    def test_dimension_insert_new(self, ch_backend_mapper, tmp_path):
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
            ("p2", "Product B"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        result = run_mapper(config, [product], backend)

        assert result.total_grain_rows == 2
        stats = result.target_stats[0]
        assert stats.inserted_new == 2
        assert stats.skipped_no_change == 0

    def test_dimension_skip_no_change(self, ch_backend_mapper, tmp_path):
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        run_mapper(config, [product], backend)

        # Run again — CH bulk_scd2 returns (0, 0) for no-change rows
        result = run_mapper(config, [product], backend)
        stats = result.target_stats[0]
        assert stats.inserted_new == 0
        assert stats.inserted_scd2 == 0

    def test_dimension_scd2_change(self, ch_backend_mapper, tmp_path):
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        run_mapper(config, [product], backend)

        # Update source data via truncate + re-insert (ClickHouse has no UPDATE)
        backend.truncate("raw_table")
        backend.bulk_upsert("raw_table", [{"product_key": "p1", "display_name": "Product A v2"}])

        result = run_mapper(config, [product], backend)
        stats = result.target_stats[0]
        assert stats.inserted_scd2 == 1

        # Verify old record is closed and new one is open
        rows = backend.execute(
            "SELECT product_key, display_name, valid_from, valid_to FROM product FINAL"
            " WHERE product_key = 'p1' ORDER BY valid_from"
        )
        cols = backend.last_columns
        assert len(rows) == 2
        old_row = dict(zip(cols, rows[0]))
        new_row = dict(zip(cols, rows[1]))
        assert old_row["valid_to"] not in (None, "")  # closed row
        assert new_row["valid_to"] in (None, "")  # open row
        assert new_row["display_name"] == "Product A v2"

    def test_fact_insert(self, ch_backend_mapper, tmp_path):
        fact = _make_fact_ontology()
        backend = _setup_db(ch_backend_mapper, [fact])
        # Use all columns as ORDER BY so distinct events (same test_key, different
        # event_id) both survive FINAL deduplication.
        self._create_raw_table(backend, "raw_events", ["test_key", "event_id", "value"], [
            ("t1", "e1", "100"),
            ("t1", "e2", "200"),
        ], primary_keys=["test_key", "event_id", "value"])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_fact_mapper()))
        result = run_mapper(config, [fact], backend)

        stats = result.target_stats[0]
        assert stats.inserted_new == 2

    def test_fact_dedup(self, ch_backend_mapper, tmp_path):
        fact = _make_fact_ontology()
        backend = _setup_db(ch_backend_mapper, [fact])
        self._create_raw_table(backend, "raw_events", ["test_key", "event_id", "value"], [
            ("t1", "e1", "100"),
        ], primary_keys=["test_key", "event_id", "value"])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_fact_mapper()))
        run_mapper(config, [fact], backend)

        # CH bulk mode: dedup is delegated to ReplacingMergeTree FINAL;
        # the mapper always counts re-inserts as inserted_new.
        result = run_mapper(config, [fact], backend)
        stats = result.target_stats[0]
        assert stats.inserted_new == 1

    def test_tolerant_mode(self, ch_backend_mapper, tmp_path):
        """Tolerant mode continues on row failures."""
        product = _make_product_ontology()

        data = _minimal_mapper()
        data["enums"] = {"state": {"GOOD": ["good"]}}
        # ENUM will fail because "Product A" is not in the state mapping
        data["targets"][0]["columns"][1] = {
            "name": "display_name", "expr": "enum(src.display_name, state)"
        }

        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        result = run_mapper(config, [product], backend, strict=False)

        assert len(result.failures) == 1
        assert result.target_stats[0].failed == 1

    def test_strict_mode(self, ch_backend_mapper, tmp_path):
        """Strict mode raises on first failure."""
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "bad_val"),
        ])

        data = _minimal_mapper()
        data["enums"] = {"state": {"GOOD": ["good"]}}
        data["targets"][0]["columns"][1]["expr"] = "enum(src.display_name, state)"

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        with pytest.raises(MapperError, match="Expression evaluation failed"):
            run_mapper(config, [product], backend, strict=True)

    def test_dry_run(self, ch_backend_mapper, tmp_path):
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        result = run_mapper(config, [product], backend, dry_run=True)

        assert result.target_stats[0].inserted_new == 1

        # Verify nothing was actually written
        rows = backend.execute("SELECT * FROM product FINAL")
        assert len(rows) == 0

    def test_fan_out_raises(self, ch_backend_mapper, tmp_path):
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        # Use a 1-to-many JOIN to produce duplicate grain rows.
        # ClickHouse FINAL deduplicates identical rows, so fan-out must come
        # from a JOIN that multiplies rows for the same grain key.
        backend.create_table("raw_table", {"product_key": "String", "display_name": "String"}, primary_keys=["product_key"])
        backend.bulk_upsert("raw_table", [{"product_key": "p1", "display_name": "Product A"}])
        backend.create_table("raw_lookup", {"lid": "String", "val": "String"}, primary_keys=["lid", "val"])
        backend.bulk_upsert("raw_lookup", [
            {"lid": "p1", "val": "x"},
            {"lid": "p1", "val": "y"},
        ])

        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "lk", "table": "raw_lookup", "columns": ["lid", "val"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "lk",
            "type": "inner",
            "on": [{"left": "src.product_key", "right": "lk.lid"}],
        }]

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        with pytest.raises(FanOutError, match="Fan-out"):
            run_mapper(config, [product], backend)

    def test_enum_transform_in_execution(self, ch_backend_mapper, tmp_path):
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Active"),
        ])

        data = _minimal_mapper()
        data["enums"] = {"states": {"ACTIVE": ["Active"]}}
        data["targets"][0]["columns"][1]["expr"] = "enum(src.display_name, states)"

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        _result = run_mapper(config, [product], backend)

        rows = backend.execute("SELECT display_name FROM product FINAL")
        assert rows[0][0] == "ACTIVE"

    def test_null_handling_left_join(self, ch_backend_mapper, tmp_path):
        """Left join with unmatched rows produces NULLs that should propagate."""
        product = _make_product_ontology()
        product.attributes.append(
            AttributeDef("description", ColumnType.STRING, SemanticHint.ATTRIBUTE)
        )
        backend = _setup_db(ch_backend_mapper, [product])

        backend.create_table("raw_table", {"product_key": "String", "display_name": "String"}, primary_keys=["product_key"])
        backend.bulk_upsert("raw_table", [{"product_key": "p1", "display_name": "Product A"}])
        backend.create_table("raw_details", {"detail_id": "String", "detail": "String"}, primary_keys=["detail_id"])
        # No rows in raw_details

        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "det", "table": "raw_details", "columns": ["detail_id", "detail"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "det",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "det.detail_id"}],
        }]
        data["targets"][0]["columns"].append(
            {"name": "description", "expr": "default(det.detail, 'none')"}
        )

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        result = run_mapper(config, [product], backend)

        assert result.target_stats[0].inserted_new == 1
        rows = backend.execute("SELECT description FROM product FINAL")
        assert rows[0][0] == "none"

    def test_filter_restricts_rows(self, ch_backend_mapper, tmp_path):
        """Filter on grain source should only process matching rows."""
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
            ("p2", "Product B"),
            ("p3", "Product A"),
        ])

        data = _minimal_mapper()
        data["sources"][0]["filter"] = 'display_name == "Product A"'
        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        result = run_mapper(config, [product], backend)

        assert result.total_grain_rows == 2
        assert result.target_stats[0].inserted_new == 2

    def test_deduplicate_avoids_fan_out(self, ch_backend_mapper, tmp_path):
        """Deduplicate on joined source prevents fan-out from duplicate rows."""
        product = _make_product_ontology()
        backend = _setup_db(ch_backend_mapper, [product])

        backend.create_table("raw_table", {"product_key": "String", "display_name": "String"}, primary_keys=["product_key"])
        backend.bulk_upsert("raw_table", [{"product_key": "p1", "display_name": "Product A"}])
        backend.create_table("raw_lookup", {"lid": "String", "category": "String"}, primary_keys=["lid"])
        # Duplicate rows for the same key (simulates append-only extraction)
        backend.bulk_upsert("raw_lookup", [
            {"lid": "p1", "category": "cat_a"},
            {"lid": "p1", "category": "cat_a"},
        ])

        data = _minimal_mapper()
        data["sources"].append(
            {"alias": "lk", "table": "raw_lookup", "columns": ["lid", "category"],
             "deduplicate": ["lid"]}
        )
        data["joins"] = [{
            "from": "src",
            "to": "lk",
            "type": "inner",
            "on": [{"left": "src.product_key", "right": "lk.lid"}],
        }]

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        result = run_mapper(config, [product], backend)

        assert result.total_grain_rows == 1
        assert result.target_stats[0].inserted_new == 1

    def test_filter_with_left_join(self, ch_backend_mapper, tmp_path):
        """Filter on joined source preserves left join NULL behaviour."""
        product = _make_product_ontology()
        product.attributes.append(
            AttributeDef("description", ColumnType.STRING, SemanticHint.ATTRIBUTE)
        )
        backend = _setup_db(ch_backend_mapper, [product])

        backend.create_table("raw_table", {"product_key": "String", "display_name": "String"}, primary_keys=["product_key"])
        backend.bulk_upsert("raw_table", [{"product_key": "p1", "display_name": "Product A"}])
        backend.create_table("raw_details", {"detail_id": "String", "detail": "String", "category": "String"}, primary_keys=["detail_id"])
        backend.bulk_upsert("raw_details", [
            {"detail_id": "p1", "detail": "Good detail", "category": "important"},
            {"detail_id": "p1", "detail": "Bad detail", "category": "irrelevant"},
        ])

        data = _minimal_mapper()
        data["sources"].append({
            "alias": "det",
            "table": "raw_details",
            "columns": ["detail_id", "detail", "category"],
            "filter": 'category == "nonexistent"',
        })
        data["joins"] = [{
            "from": "src",
            "to": "det",
            "type": "left",
            "on": [{"left": "src.product_key", "right": "det.detail_id"}],
        }]
        data["targets"][0]["columns"].append(
            {"name": "description", "expr": "default(det.detail, 'none')"}
        )

        config = parse_mapper_yaml(_write_yaml(tmp_path, data))
        result = run_mapper(config, [product], backend)

        assert result.target_stats[0].inserted_new == 1
        rows = backend.execute("SELECT description FROM product FINAL")
        assert rows[0][0] == "none"


# ===========================================================================
# 8b. UPSERT strategy
# ===========================================================================

class TestUpsertStrategy:
    def _create_raw_table(self, backend, table_name, columns, rows, primary_keys=None):
        pks = primary_keys if primary_keys is not None else [columns[0]]
        backend.create_table(table_name, {c: "String" for c in columns}, primary_keys=pks)
        if rows:
            backend.bulk_upsert(table_name, [dict(zip(columns, r)) for r in rows])

    def test_resolve_write_context_upsert(self):
        """Entity without history should resolve to UPSERT strategy."""
        from semantic.mapper_types import TargetDef
        entity = _make_product_no_history_ontology()
        target = TargetDef(entity_name="Product")
        ctx = _resolve_write_context(target, [entity])
        assert ctx.strategy == "UPSERT"
        assert ctx.key_col == "product_key"

    def test_resolve_write_context_scd2_with_history(self):
        """Entity with history should still resolve to SCD2."""
        from semantic.mapper_types import TargetDef
        entity = _make_product_ontology()
        target = TargetDef(entity_name="Product")
        ctx = _resolve_write_context(target, [entity])
        assert ctx.strategy == "SCD2"

    def test_upsert_insert_new(self, ch_backend_mapper, tmp_path):
        """UPSERT should insert new rows."""
        product = _make_product_no_history_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
            ("p2", "Product B"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        result = run_mapper(config, [product], backend)

        assert result.total_grain_rows == 2
        stats = result.target_stats[0]
        assert stats.write_strategy == "UPSERT"
        assert stats.inserted_new == 2

    def test_upsert_update_existing(self, ch_backend_mapper, tmp_path):
        """UPSERT should update existing rows when tracked columns change."""
        product = _make_product_no_history_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        run_mapper(config, [product], backend)

        # Update source data via truncate + re-insert (ClickHouse has no UPDATE)
        backend.truncate("raw_table")
        backend.bulk_upsert("raw_table", [{"product_key": "p1", "display_name": "Product A v2"}])

        result = run_mapper(config, [product], backend)
        stats = result.target_stats[0]
        # CH bulk mode: all upserts counted as inserted_new
        assert stats.inserted_new == 1

        # Verify only one row exists via FINAL (higher _version supersedes)
        rows = backend.execute("SELECT product_key, display_name FROM product FINAL")
        cols = backend.last_columns
        assert len(rows) == 1
        assert dict(zip(cols, rows[0]))["display_name"] == "Product A v2"

    def test_upsert_skip_no_change(self, ch_backend_mapper, tmp_path):
        """UPSERT with unchanged data: CH bulk mode counts as inserted_new."""
        product = _make_product_no_history_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        run_mapper(config, [product], backend)

        result = run_mapper(config, [product], backend)
        stats = result.target_stats[0]
        # CH bulk_upsert always counts as inserted_new; dedup via ReplacingMergeTree FINAL
        assert stats.inserted_new == 1
        assert stats.skipped_no_change == 0
        assert stats.updated == 0
        # Verify exactly one row in DB
        rows = backend.execute("SELECT product_key FROM product FINAL")
        assert len(rows) == 1

    def test_upsert_no_valid_from_valid_to(self, ch_backend_mapper, tmp_path):
        """UPSERT rows should not have valid_from/valid_to columns."""
        product = _make_product_no_history_ontology()
        backend = _setup_db(ch_backend_mapper, [product])
        self._create_raw_table(backend, "raw_table", ["product_key", "display_name"], [
            ("p1", "Product A"),
        ])

        config = parse_mapper_yaml(_write_yaml(tmp_path, _minimal_mapper()))
        run_mapper(config, [product], backend)

        backend.execute("SELECT * FROM product FINAL")
        columns = backend.last_columns
        assert "valid_from" not in columns
        assert "valid_to" not in columns


# ===========================================================================
# 9. Topological sort
# ===========================================================================

class TestTopologicalSort:
    def test_no_dependencies(self):
        a = MapperConfig(name="a")
        b = MapperConfig(name="b")
        result = topological_sort([a, b])
        names = [m.name for m in result]
        assert set(names) == {"a", "b"}

    def test_dependency_order(self):
        a = MapperConfig(name="a")
        b = MapperConfig(name="b", depends_on=["a"])
        result = topological_sort([b, a])
        names = [m.name for m in result]
        assert names.index("a") < names.index("b")

    def test_cycle_detection(self):
        a = MapperConfig(name="a", depends_on=["b"])
        b = MapperConfig(name="b", depends_on=["a"])
        with pytest.raises(ValueError, match="cycle"):
            topological_sort([a, b])


# ===========================================================================
# 10. Integration — real YAML files pass validation
# ===========================================================================

class TestIntegration:
    def test_schema_errors_skip_semantic_phase(self, tmp_path):
        bad = {"mapper": "BadName", "grain": "src", "sources": [], "targets": []}
        p = _write_yaml(tmp_path, bad)
        errs = validate_mapper_yaml(p)
        assert all(e.rule == "schema" for e in errs)
        assert len(errs) >= 1

    def test_valid_file_passes(self, tmp_path):
        p = _write_yaml(tmp_path, _minimal_mapper())
        assert validate_mapper_yaml(p) == []

    @pytest.mark.parametrize(
        "yaml_file",
        sorted(MAPPERS_DIR.glob("*.yaml")) if MAPPERS_DIR.is_dir() else [],
        ids=lambda p: p.name,
    )
    def test_existing_mapper_yamls_pass(self, yaml_file):
        """All checked-in mapper YAMLs must pass validation."""
        errs = validate_mapper_yaml(yaml_file)
        assert errs == [], f"{yaml_file.name}: {[f'[{e.rule}] {e.path}: {e.message}' for e in errs]}"

    def test_load_all_mappers(self, tmp_path):
        _write_yaml(tmp_path, _minimal_mapper(), "m1.yaml")
        _write_yaml(tmp_path, _minimal_mapper(mapper="other"), "m2.yaml")
        mappers = load_all_mappers(tmp_path)
        assert len(mappers) == 2

    def test_load_mapper_by_name(self, tmp_path):
        _write_yaml(tmp_path, _minimal_mapper(), "m1.yaml")
        m = load_mapper_by_name("test_mapper", tmp_path)
        assert m is not None
        assert m.name == "test_mapper"

    def test_load_mapper_by_name_not_found(self, tmp_path):
        _write_yaml(tmp_path, _minimal_mapper(), "m1.yaml")
        assert load_mapper_by_name("nonexistent", tmp_path) is None
