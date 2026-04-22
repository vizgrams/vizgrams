# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Query definition dataclasses, YAML parsing, and validation."""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

_log = logging.getLogger(__name__)

from core.validation import ValidationError, validate_schema
from semantic.expression import parse_expression_str as _parse_expression_str
from semantic.types import Cardinality, EntityDef, SemanticHint


@dataclass
class QueryAttribute:
    parts: list[str]         # ['created_at'] or ['Product', 'display_name']
    expr_str: str | None = None   # set when the attribute is an expression (e.g. format_date(...))
    label: str | None = None      # display label for expression attributes

    @property
    def is_traversal(self) -> bool:
        """True when the attribute uses dot notation."""
        return len(self.parts) > 1

    @property
    def raw_field(self) -> str:
        """The YAML-as-written dotted string."""
        return ".".join(self.parts)


@dataclass
class PaginationDef:
    page_size: int = 100
    page: int = 1


@dataclass
class SliceDef:
    field: str              # "team", "Team.display_name", "merged_at"
    alias: str | None = None         # display alias for the column
    format_pattern: str | None = None  # e.g. "YYYY-WW", "YYYY-MM" (for timestamps)
    order_position: int | None = None  # 1-indexed ORDER BY position (None = not in ORDER BY)
    order_direction: str = "asc"       # "asc" | "desc"

    @property
    def inferred_grain(self) -> str | None:
        """Infer a grain string from format_pattern (for window unit matching)."""
        if self.format_pattern is None:
            return None
        p = self.format_pattern
        if "WW" in p:
            return "week"
        if "DD" in p:
            return "day"
        if "MM" in p:
            return "month"
        return None


@dataclass
class ThresholdDef:
    op: str      # <  <=  >  >=  ==  !=
    value: float
    status: str


@dataclass
class FormatDef:
    type: str                   # number | percent | duration
    pattern: str | None = None  # e.g. "0.0", "0.0%", "#,##0.0"
    unit: str | None = None     # for duration: hours | days | weeks | minutes | seconds


@dataclass
class WindowDef:
    method: str               # weighted | simple | cumulative | lag | lead
    unit: str                 # day | week | month | quarter | rows
    frame: int | None = None  # required for weighted/simple
    offset: int | None = None # required for lag/lead


@dataclass
class QueryMetric:
    field: str
    rollup: str          # sum | avg | min | max | count | count_distinct
    window: WindowDef | None = None
    thresholds: list[ThresholdDef] = field(default_factory=list)
    format: FormatDef | None = None
    order_position: int | None = None
    order_direction: str = "asc"


@dataclass
class RatioComponent:
    field: str
    rollup: str          # sum | avg | min | max | count | count_distinct


@dataclass
class RatioMetric:
    numerator: RatioComponent
    denominator: RatioComponent
    thresholds: list[ThresholdDef] = field(default_factory=list)
    format: FormatDef | None = None
    order_position: int | None = None
    order_direction: str = "asc"


@dataclass
class ParameterDef:
    name: str
    type: str  # string | number | duration
    label: str | None = None
    default: str | None = None  # None = no default; param must be supplied unless optional=True
    optional: bool = False  # if True and no value provided, filters referencing this param are dropped


@dataclass
class QueryDef:
    name: str
    entity: str
    detail: bool = False
    attributes: list[QueryAttribute] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    parameters: list[ParameterDef] = field(default_factory=list)
    pagination: PaginationDef = field(default_factory=PaginationDef)
    slices: list[SliceDef] = field(default_factory=list)
    metrics: dict[str, QueryMetric | RatioMetric] = field(default_factory=dict)
    order_by: list[tuple[str, str]] = field(default_factory=list)  # [(column, "ASC"|"DESC")]

    @property
    def is_aggregate(self) -> bool:
        """True if this query groups/aggregates (formerly a pivot)."""
        return bool(self.slices or self.metrics)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_FORMAT_TIME_RE = re.compile(r'^format_time\((\w[\w.]*)\s*,\s*["\']([^"\']+)["\']\)$')

_THRESHOLD_OPS = {
    "<":  lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">":  lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def evaluate_threshold(value, thresholds: list[ThresholdDef]) -> str | None:
    """Evaluate thresholds in order and return the first matching status.

    Returns None if value is None or no threshold matches.
    """
    if value is None:
        return None
    for t in thresholds:
        if _THRESHOLD_OPS[t.op](value, t.value):
            return t.status
    return None


def _parse_order(val) -> tuple[int | None, str]:
    """Parse "1, asc", "1", "desc" → (position_or_None, direction)."""
    s = str(val).strip()
    parts = [p.strip() for p in s.split(",")]
    position = None
    direction = "asc"
    for p in parts:
        if p.isdigit():
            position = int(p)
        elif p in ("asc", "desc"):
            direction = p
    return position, direction


def _parse_attribute_item(item) -> SliceDef:
    """Parse {alias: expr, order?: ...} dict into SliceDef."""
    if not isinstance(item, dict):
        raise ValueError(f"Attribute must be a dict {{alias: expr}}, got {item!r}")
    alias = None
    expr_str = None
    order_position = None
    order_direction = "asc"
    _ATTR_RESERVED = {"order"}
    for k, v in item.items():
        if k == "order":
            order_position, order_direction = _parse_order(v)
        elif k in ("thresholds", "window"):
            raise ValueError(
                f"Key {k!r} is not valid in an attribute — did you mean to put this in measures:?"
            )
        elif alias is not None:
            raise ValueError(f"Attribute item must have exactly one alias key: {item!r}")
        else:
            alias = str(k)
            expr_str = str(v)
    if alias is None or expr_str is None:
        raise ValueError(f"Attribute item must have exactly one alias key: {item!r}")
    m = _FORMAT_TIME_RE.match(expr_str.strip())
    if m:
        return SliceDef(
            field=m.group(1),
            alias=alias,
            format_pattern=m.group(2),
            order_position=order_position,
            order_direction=order_direction,
        )
    return SliceDef(
        field=expr_str.strip(),
        alias=alias,
        order_position=order_position,
        order_direction=order_direction,
    )


def _agg_field_name(agg_expr) -> str:
    """Extract the field name string from an AggExpr (must wrap a FieldRef)."""
    from semantic.expression import FieldRef
    if isinstance(agg_expr.expr, FieldRef):
        return ".".join(agg_expr.expr.parts)
    raise ValueError(
        f"Aggregate argument must be a field reference, got {agg_expr.expr!r}"
    )


def _parse_measure_expr(
    expr_str: str,
    window_def: WindowDef | None = None,
) -> "QueryMetric | RatioMetric":
    """Parse an `expr:` measure string into a QueryMetric or RatioMetric.

    Supported forms:
      agg(field)                → QueryMetric(field, rollup)
      agg(field) / agg(field)  → RatioMetric(numerator, denominator)
    """
    from semantic.expression import AggExpr, BinOp, parse_expression_str

    expr = parse_expression_str(expr_str)

    if (
        isinstance(expr, BinOp)
        and expr.op == "/"
        and isinstance(expr.left, AggExpr)
        and isinstance(expr.right, AggExpr)
    ):
        return RatioMetric(
            numerator=RatioComponent(
                field=_agg_field_name(expr.left),
                rollup=expr.left.func.value,
            ),
            denominator=RatioComponent(
                field=_agg_field_name(expr.right),
                rollup=expr.right.func.value,
            ),
        )

    if isinstance(expr, AggExpr):
        return QueryMetric(
            field=_agg_field_name(expr),
            rollup=expr.func.value,
            window=window_def,
        )

    raise ValueError(
        f"Unsupported measure expression {expr_str!r}: "
        "must be agg(field) or agg(field) / agg(field)"
    )


def _parse_format_def(data: dict) -> FormatDef:
    return FormatDef(
        type=data["type"],
        pattern=data.get("pattern"),
        unit=data.get("unit"),
    )


def _parse_measure_list_item(item: dict) -> tuple[str, "QueryMetric | RatioMetric"]:
    """Parse {alias: {expr: ..., format?: ..., order?: ..., window?: ..., thresholds?: ...}} into (alias, metric)."""
    if len(item) != 1:
        raise ValueError(f"Measure item must have exactly one alias key: {item!r}")
    alias, body = next(iter(item.items()))
    alias = str(alias)
    if not isinstance(body, dict) or "expr" not in body:
        raise ValueError(
            f"Measure {alias!r} value must be a dict with an 'expr' key, got {body!r}"
        )
    expr_str = str(body["expr"])
    format_def = _parse_format_def(body["format"]) if "format" in body else None
    order_position, order_direction = _parse_order(body["order"]) if "order" in body else (None, "asc")
    window_def = None
    if "window" in body:
        w = body["window"]
        window_def = WindowDef(
            method=w["method"], unit=w["unit"],
            frame=w.get("frame"), offset=w.get("offset"),
        )
    thresholds = [ThresholdDef(op=t["op"], value=t["value"], status=t["status"]) for t in body.get("thresholds", [])]
    metric = _parse_measure_expr(expr_str, window_def)
    metric.format = format_def
    if thresholds:
        metric.thresholds = thresholds
    metric.order_position = order_position
    metric.order_direction = order_direction
    return alias, metric


def parse_query_dict(data: dict) -> QueryDef:
    """Parse a query definition dict (already loaded from YAML/JSON) into a QueryDef."""
    name = data.get("name", "")

    # Normalise key aliases
    entity_name = data.get("root") or data.get("entity", "")
    filters_raw = data.get("where") or data.get("filters", [])
    measures_raw = data.get("measures", [])

    # Parse attributes: list of {alias: expr, order?: ...} dicts → SliceDef list
    # Also handle old-format plain strings for detail queries (QueryAttribute)
    is_detail = bool(data.get("detail", False))
    attributes_raw = data.get("attributes", [])
    slices: list[SliceDef] = []
    attributes: list[QueryAttribute] = []

    for attr_item in attributes_raw:
        if isinstance(attr_item, dict):
            if is_detail:
                # In detail queries {alias: path} is a labelled traversal attribute, not a slice
                alias = next((str(k) for k in attr_item if k != "order"), None)
                expr = str(attr_item[alias]) if alias else None
                if alias and expr:
                    attributes.append(QueryAttribute(parts=expr.split("."), label=alias))
            else:
                # Aggregate format: {alias: expr, order?: ...} → SliceDef
                slices.append(_parse_attribute_item(attr_item))
        elif isinstance(attr_item, str):
            # Old/detail format: plain string → QueryAttribute
            attr_str = attr_item
            if "(" in attr_str:
                m = re.match(r'\w+\((\w+)', attr_str)
                derived = m.group(1) if m else attr_str
                attributes.append(QueryAttribute(parts=[derived], expr_str=attr_str, label=derived))
            else:
                attributes.append(QueryAttribute(parts=attr_str.split(".")))
        else:
            attr_str = str(attr_item)
            attributes.append(QueryAttribute(parts=attr_str.split(".")))

    # Parse measures: list of {alias: expr, ...} dicts OR legacy dict-of-dicts
    metrics: dict[str, QueryMetric | RatioMetric] = {}
    if isinstance(measures_raw, list):
        for item in measures_raw:
            if isinstance(item, dict):
                alias, metric = _parse_measure_list_item(item)
                metrics[alias] = metric
    elif isinstance(measures_raw, dict):
        # Legacy dict-of-dicts format (kept for backward compat during transition)
        for metric_name, m in measures_raw.items():
            thresholds = [
                ThresholdDef(op=t["op"], value=t["value"], status=t["status"])
                for t in m.get("thresholds", [])
            ]
            if "expr" in m:
                window = None
                if "window" in m:
                    w = m["window"]
                    window = WindowDef(
                        method=w["method"],
                        unit=w["unit"],
                        frame=w.get("frame"),
                        offset=w.get("offset"),
                    )
                metric = _parse_measure_expr(m["expr"], window)
                if thresholds:
                    if isinstance(metric, QueryMetric):
                        metric = QueryMetric(
                            field=metric.field, rollup=metric.rollup,
                            window=metric.window, thresholds=thresholds,
                        )
                    else:
                        metric = RatioMetric(
                            numerator=metric.numerator, denominator=metric.denominator,
                            thresholds=thresholds,
                        )
                metrics[metric_name] = metric

    pagination_data = data.get("pagination", {})
    pagination = PaginationDef(
        page_size=pagination_data.get("page_size", 100),
        page=pagination_data.get("page", 1),
    )

    # Top-level order: block — [{col: dir}, ...] e.g. [{airline: asc}, {count: desc}]
    # Also accepts the inline-query dict form [{column: col, direction: dir}] for API clients.
    order_by: list[tuple[str, str]] = []
    for item in data.get("order", []):
        if isinstance(item, dict):
            # {col_name: dir} form (YAML) — take the first key
            for col, dir_ in item.items():
                direction = str(dir_).upper()
                if direction not in ("ASC", "DESC"):
                    direction = "ASC"
                order_by.append((str(col), direction))
                break
    # Legacy API client form: order_by: [{column: ..., direction: ...}]
    for item in data.get("order_by", []):
        if isinstance(item, dict) and "column" in item:
            direction = str(item.get("direction", "ASC")).upper()
            if direction not in ("ASC", "DESC"):
                direction = "ASC"
            order_by.append((str(item["column"]), direction))

    parameters = []
    for p in data.get("params", []):
        parameters.append(ParameterDef(
            name=p["name"],
            type=p.get("type", "string"),
            label=p.get("label"),
            default=str(p["default"]) if p.get("default") is not None else None,
            optional=bool(p.get("optional", False)),
        ))

    return QueryDef(
        name=name,
        entity=entity_name,
        detail=bool(data.get("detail", False)),
        attributes=attributes,
        filters=filters_raw,
        parameters=parameters,
        pagination=pagination,
        slices=slices,
        metrics=metrics,
        order_by=order_by,
    )


def parse_query_yaml(path: str | Path) -> QueryDef:
    """Parse a query YAML file into a QueryDef."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)
    return parse_query_dict(data)


def load_all_queries(queries_dir: str | Path) -> list[QueryDef]:
    """Load all query definitions from a directory."""
    queries_dir = Path(queries_dir)
    result = []
    for path in sorted(queries_dir.glob("*.yaml")):
        try:
            result.append(parse_query_yaml(path))
        except Exception as exc:
            _log.warning("Skipping malformed query file %s: %s", path.name, exc)
    return result


def load_query_by_name(name: str, queries_dir: str | Path) -> QueryDef | None:
    """Load a single query by name (expects <name>.yaml)."""
    path = Path(queries_dir) / f"{name}.yaml"
    if not path.is_file():
        return None
    return parse_query_yaml(path)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _collect_entity_attributes(
    entity: EntityDef,
    extra_names: set[str] | None = None,
) -> set[str]:
    """Return all direct attribute names on an entity (identity + attributes + features)."""
    names = set()
    for a in entity.identity:
        names.add(a.name)
    for a in entity.attributes:
        names.add(a.name)
    if extra_names:
        names.update(extra_names)
    return names


def _timestamp_fields(entity: EntityDef) -> set[str]:
    """Return attribute names that have TIMESTAMP semantic."""
    names = set()
    for a in entity.attributes:
        if a.semantic == SemanticHint.TIMESTAMP:
            names.add(a.name)
    return names


def _validate_attribute_traversal(
    attr: QueryAttribute,
    root_entity: EntityDef,
    entities: dict[str, EntityDef],
    extra_root_names: set[str] | None = None,
) -> list[str]:
    """Validate a dot-notation attribute by traversing MANY_TO_ONE relations.

    Returns list of error messages (empty = valid).
    """
    parts = attr.parts
    attr_str = ".".join(parts)

    if len(parts) == 1:
        all_attrs = _collect_entity_attributes(root_entity, extra_names=extra_root_names)
        if parts[0] not in all_attrs:
            return [f"attribute '{attr_str}' not found on entity '{root_entity.name}'"]
        return []

    if parts[0] == root_entity.name:
        field_name = parts[-1]
        all_attrs = _collect_entity_attributes(root_entity, extra_names=extra_root_names)
        if field_name not in all_attrs:
            return [
                f"attribute '{attr_str}': field '{field_name}' not found on entity '{root_entity.name}'"
            ]
        return []

    current_entity = root_entity
    for entity_name in parts[:-1]:
        rel = next(
            (r for r in current_entity.relations
             if r.target == entity_name and r.cardinality == Cardinality.MANY_TO_ONE),
            None,
        )
        if rel is None:
            return [
                f"attribute '{attr_str}': no MANY_TO_ONE relation from '{current_entity.name}' "
                f"to entity '{entity_name}'"
            ]
        next_entity = entities.get(entity_name)
        if next_entity is None:
            return [
                f"attribute '{attr_str}': entity '{entity_name}' not found in ontology"
            ]
        current_entity = next_entity

    final_attr = parts[-1]
    all_attrs = _collect_entity_attributes(current_entity)
    if final_attr not in all_attrs:
        return [
            f"attribute '{attr_str}': field '{final_attr}' not found on entity '{current_entity.name}'"
        ]
    return []


def _validate_slice_traversal(
    slice_def: SliceDef,
    root_entity: EntityDef,
    entities: dict[str, EntityDef],
    extra_names: set[str] | None = None,
) -> list[str]:
    """Validate a dot-notation slice field by traversing MANY_TO_ONE relations.

    Returns list of error messages (empty = valid).
    """
    parts = slice_def.field.split(".")
    if len(parts) == 1:
        all_attrs = _collect_entity_attributes(root_entity, extra_names=extra_names)
        if parts[0] not in all_attrs:
            return [f"field '{slice_def.field}' not found on entity '{root_entity.name}'"]
        return []

    current_entity = root_entity
    for part in parts[:-1]:
        if part[0].isupper():
            rel = next(
                (r for r in current_entity.relations
                 if r.target == part and r.cardinality == Cardinality.MANY_TO_ONE),
                None,
            )
            if rel is None:
                return [
                    f"slice '{slice_def.field}': no MANY_TO_ONE relation from '{current_entity.name}' "
                    f"to entity '{part}'"
                ]
            next_entity = entities.get(part)
        else:
            rel = next(
                (r for r in current_entity.relations
                 if r.name == part and r.cardinality == Cardinality.MANY_TO_ONE),
                None,
            )
            if rel is None:
                return [
                    f"slice '{slice_def.field}': '{part}' is not a MANY_TO_ONE (belongs_to) "
                    f"relation on '{current_entity.name}'"
                ]
            if rel.dynamic_field is not None:
                # Dynamic relation — field is resolved at runtime across all entities.
                # Validate that at least one entity has the final field.
                final_attr = parts[-1]
                any_has_field = any(
                    final_attr in {a.name for a in e.all_base_columns}
                    for e in entities.values()
                )
                if not any_has_field:
                    return [
                        f"slice '{slice_def.field}': field '{final_attr}' not found "
                        f"on any entity in the ontology"
                    ]
                return []
            next_entity = entities.get(rel.target)
        if next_entity is None:
            return [
                f"slice '{slice_def.field}': entity '{rel.target}' not found in ontology"
            ]
        current_entity = next_entity

    final_attr = parts[-1]
    all_attrs = _collect_entity_attributes(current_entity)
    if final_attr not in all_attrs:
        return [
            f"slice '{slice_def.field}': field '{final_attr}' not found on entity '{current_entity.name}'"
        ]
    return []


def validate_query_yaml(
    path: str | Path,
    entities: dict[str, EntityDef],
    features_by_entity: dict | None = None,
) -> list[ValidationError]:
    """Validate a query YAML file.

    Phase 1: JSON Schema structural check.
    Phase 2: semantic checks (merged scan + pivot validation rules).
    """
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    # Phase 1: JSON Schema
    errors = validate_schema(data, "query")
    if errors:
        return errors

    # Phase 2: semantic — normalise key aliases first
    query_name = data.get("name", "")
    entity_name = data.get("root") or data.get("entity", "")
    filters_list = data.get("where") or data.get("filters", [])
    attributes_raw = data.get("attributes", [])
    measures_raw = data.get("measures", [])

    # query_name_matches_filename
    if query_name != path.stem:
        errors.append(ValidationError(
            path="query",
            message=f"query name '{query_name}' must match filename '{path.stem}'",
            rule="query_name_matches_filename",
        ))

    # unknown_entity
    root_entity = entities.get(entity_name)
    if root_entity is None:
        errors.append(ValidationError(
            path="entity",
            message=f"entity '{entity_name}' not found in ontology",
            rule="unknown_entity",
        ))
        return errors

    feature_names: set[str] = set()
    if features_by_entity:
        feature_names = set((features_by_entity.get(entity_name) or {}).keys())
    root_attrs = _collect_entity_attributes(root_entity, extra_names=feature_names)

    # Validate attributes
    for i, attr_item in enumerate(attributes_raw):
        if isinstance(attr_item, dict):
            # New format: {alias: expr, order?: ...} — validate via SliceDef
            try:
                slice_def = _parse_attribute_item(attr_item)
            except ValueError as e:
                errors.append(ValidationError(
                    path=f"attributes[{i}]",
                    message=str(e),
                    rule="invalid_attribute_item",
                ))
                continue
            traversal_errors = _validate_slice_traversal(
                slice_def, root_entity, entities, extra_names=feature_names
            )
            for msg in traversal_errors:
                rule = "invalid_attribute_traversal" if "." in slice_def.field else "invalid_attribute_field"
                errors.append(ValidationError(
                    path=f"attributes[{i}]",
                    message=msg,
                    rule=rule,
                ))
            continue

        attr_str = attr_item if isinstance(attr_item, str) else str(attr_item)
        if "(" in attr_str:
            # Expression attribute — validate it parses as a known function call
            from semantic.expression import FuncCallExpr, parse_expression_str
            _SUPPORTED_ATTR_FUNCS = {"format_date"}
            try:
                expr = parse_expression_str(attr_str)
                if not isinstance(expr, FuncCallExpr):
                    errors.append(ValidationError(
                        path=f"attributes[{i}]",
                        message=f"expression attribute must be a function call, got {type(expr).__name__}",
                        rule="invalid_attribute_expr",
                    ))
                elif expr.name not in _SUPPORTED_ATTR_FUNCS:
                    errors.append(ValidationError(
                        path=f"attributes[{i}]",
                        message=(
                            f"unsupported attribute function '{expr.name}'; "
                            f"supported: {sorted(_SUPPORTED_ATTR_FUNCS)}"
                        ),
                        rule="invalid_attribute_expr",
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    path=f"attributes[{i}]",
                    message=f"failed to parse attribute expression: {e}",
                    rule="invalid_attribute_expr",
                ))
            continue
        attr = QueryAttribute(parts=attr_str.split("."))
        traversal_errors = _validate_attribute_traversal(
            attr, root_entity, entities, extra_root_names=feature_names
        )
        for msg in traversal_errors:
            rule = "invalid_attribute_traversal" if attr.is_traversal else "invalid_attribute_field"
            errors.append(ValidationError(
                path=f"attributes[{i}]",
                message=msg,
                rule=rule,
            ))

    # Build a map of declared params for placeholder substitution during validation
    param_defs_for_validation = {}
    for p in data.get("params", []):
        param_defs_for_validation[p["name"]] = p.get("type", "string")

    def _resolve_filter_placeholders(s: str) -> str:
        """Substitute {param_name} with type-safe dummy values for parse validation."""
        def _sub(m):
            ptype = param_defs_for_validation.get(m.group(1), "string")
            if ptype == "number":
                return "0"
            if ptype == "duration":
                return "1"
            return "placeholder"
        return re.sub(r'\{(\w+)\}', _sub, s)

    # invalid_filter_expression
    for i, filter_str in enumerate(filters_list):
        try:
            _parse_expression_str(_resolve_filter_placeholders(filter_str))
        except Exception as e:
            errors.append(ValidationError(
                path=f"filters[{i}]",
                message=f"invalid filter expression: {e}",
                rule="invalid_filter_expression",
            ))

    # Parse slices from attributes list (new dict format) for aggregate validation
    slices: list[SliceDef] = []
    for attr_item in attributes_raw:
        if isinstance(attr_item, dict):
            try:
                slices.append(_parse_attribute_item(attr_item))
            except ValueError:
                pass

    # Parse measures
    if isinstance(measures_raw, list):
        metrics_data_for_validation = {}
        for item in measures_raw:
            if not isinstance(item, dict) or len(item) != 1:
                continue
            alias, body = next(iter(item.items()))
            alias = str(alias)
            if not isinstance(body, dict) or "expr" not in body:
                continue
            entry = {"expr": str(body["expr"])}
            if "window" in body:
                entry["window"] = body["window"]
            if "thresholds" in body:
                entry["thresholds"] = body["thresholds"]
            metrics_data_for_validation[alias] = entry
    else:
        metrics_data_for_validation = measures_raw or {}

    is_aggregate = bool(slices or metrics_data_for_validation)

    if not is_aggregate and not attributes_raw:
        errors.append(ValidationError(
            path="attributes",
            message="detail query must define at least one attribute",
            rule="detail_requires_attributes",
        ))

    if is_aggregate:
        ts_fields = _timestamp_fields(root_entity)

        # Slice validation
        for i, slice_def in enumerate(slices):
            prefix = f"attributes[{i}]"
            traversal_errors = _validate_slice_traversal(
                slice_def, root_entity, entities, extra_names=feature_names
            )
            for msg in traversal_errors:
                rule = "invalid_slice_traversal" if "." in slice_def.field else "invalid_slice_field"
                errors.append(ValidationError(path=prefix, message=msg, rule=rule))

            if traversal_errors:
                continue

            is_bare_field = "." not in slice_def.field
            is_feature_field = slice_def.field in feature_names
            if is_bare_field and not is_feature_field:
                is_ts = slice_def.field in ts_fields

                has_time_spec = slice_def.format_pattern is not None
                if is_ts and not has_time_spec:
                    errors.append(ValidationError(
                        path=prefix,
                        message=(
                            f"TIMESTAMP field '{slice_def.field}' must specify a time grain "
                            f"(use format_time(field, pattern))"
                        ),
                        rule="timestamp_requires_grain",
                    ))

                if not is_ts and has_time_spec:
                    errors.append(ValidationError(
                        path=prefix,
                        message=(
                            f"field '{slice_def.field}' is not a TIMESTAMP and must not have a time grain"
                        ),
                        rule="non_timestamp_forbids_grain",
                    ))

        # Metric validation
        for metric_name, metric_def in metrics_data_for_validation.items():
            if "expr" not in metric_def:
                errors.append(ValidationError(
                    path=f"metrics.{metric_name}",
                    message="measure must have an 'expr' key",
                    rule="missing_expr",
                ))
                continue

            try:
                parsed = _parse_measure_expr(metric_def["expr"])
            except ValueError as e:
                errors.append(ValidationError(
                    path=f"metrics.{metric_name}.expr",
                    message=str(e),
                    rule="invalid_measure_expr",
                ))
                continue

            if isinstance(parsed, RatioMetric):
                for component_name, component in [
                    ("numerator", parsed.numerator),
                    ("denominator", parsed.denominator),
                ]:
                    if component.field not in root_attrs:
                        errors.append(ValidationError(
                            path=f"metrics.{metric_name}.{component_name}",
                            message=f"field '{component.field}' not found on entity '{entity_name}'",
                            rule="invalid_ratio_component_field",
                        ))
                continue

            if parsed.field and "." not in parsed.field and parsed.field not in root_attrs:
                errors.append(ValidationError(
                    path=f"metrics.{metric_name}.field",
                    message=f"field '{parsed.field}' not found on entity '{entity_name}'",
                    rule="invalid_metric_field",
                ))

            window_data = metric_def.get("window")
            if window_data is None:
                continue

            method = window_data.get("method", "")
            unit = window_data.get("unit", "")
            frame = window_data.get("frame")
            offset = window_data.get("offset")
            rollup = parsed.rollup
            window_path = f"metrics.{metric_name}.window"

            if method == "weighted" and rollup != "avg":
                errors.append(ValidationError(
                    path=window_path,
                    message=f"method 'weighted' requires rollup 'avg', got '{rollup}'",
                    rule="weighted_requires_avg",
                ))

            if method in ("weighted", "simple") and frame is None:
                errors.append(ValidationError(
                    path=window_path,
                    message=f"method '{method}' requires a frame value",
                    rule="window_requires_frame",
                ))

            if method in ("lag", "lead") and offset is None:
                errors.append(ValidationError(
                    path=window_path,
                    message=f"method '{method}' requires an offset value",
                    rule="lag_lead_requires_offset",
                ))

            if unit != "rows":
                has_matching_grain = any(s.inferred_grain == unit for s in slices)
                if not has_matching_grain:
                    errors.append(ValidationError(
                        path=window_path,
                        message=f"unit '{unit}' requires a slice with time_grain '{unit}'",
                        rule="window_unit_must_match_slice_grain",
                    ))

            if unit == "rows" and len(slices) < 2:
                errors.append(ValidationError(
                    path=window_path,
                    message="unit 'rows' requires at least 2 slices",
                    rule="window_rows_requires_multiple_slices",
                ))

    return errors
