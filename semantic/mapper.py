# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Mapper YAML parser and validator for the semantic layer."""

from pathlib import Path

import yaml

from core.validation import ValidationError, validate_schema
from engine.filter_compiler import collect_filter_column_refs
from engine.python_evaluator import collect_enum_refs, collect_refs
from semantic.expression import parse_expression_str as _parse_expression_str
from semantic.mapper_types import (
    EnumMapping,
    JoinCondition,
    JoinDef,
    JoinType,
    MapperConfig,
    RowGroup,
    SourceDef,
    TargetColumn,
    TargetDef,
)


def parse_mapper_dict(data: dict) -> MapperConfig:
    """Parse a mapper dict (already YAML-loaded) into a MapperConfig."""
    enums = []
    for enum_name, enum_def in (data.get("enums") or {}).items():
        enums.append(EnumMapping(name=enum_name, mapping=enum_def))

    sources = []
    for src in data.get("sources", []):
        sources.append(SourceDef(
            alias=src["alias"],
            table=src.get("table"),
            columns=src["columns"],
            union=src.get("union"),
            filter=src.get("filter"),
            deduplicate=src.get("deduplicate"),
            static=src.get("static"),
        ))

    joins = []
    for join in data.get("joins", []):
        conditions = []
        for cond in join.get("on") or join[True]:
            conditions.append(JoinCondition(
                left=cond["left"],
                right=cond["right"],
                operator=cond.get("operator", "eq"),
                json_path=cond.get("json_path"),
                prefix=cond.get("prefix"),
            ))
        joins.append(JoinDef(
            from_alias=join["from"],
            to_alias=join["to"],
            join_type=JoinType(join["type"]),
            on=conditions,
        ))

    targets = []
    for tgt in data.get("targets", []):
        columns = []
        for col in tgt.get("columns", []):
            columns.append(TargetColumn(name=col["name"], expression=col["expr"]))
        rows = []
        for rg in tgt.get("rows", []):
            rg_joins = [
                JoinDef(
                    from_alias=rg["from"],
                    to_alias=join["to"],
                    join_type=JoinType(join["type"]),
                    on=[JoinCondition(
                        left=c["left"],
                        right=c["right"],
                        operator=c.get("operator", "eq"),
                        json_path=c.get("json_path"),
                        prefix=c.get("prefix"),
                    ) for c in join.get("on", [])],
                )
                for join in rg.get("joins", [])
            ]
            rows.append(RowGroup(
                from_alias=rg["from"],
                joins=rg_joins,
                columns=[TargetColumn(name=c["name"], expression=c["expr"])
                         for c in rg.get("columns", [])],
            ))
        targets.append(TargetDef(
            entity_name=tgt["entity"],
            columns=columns,
            rows=rows,
        ))

    return MapperConfig(
        name=data["mapper"],
        description=data.get("description"),
        depends_on=data.get("depends_on", []),
        grain=data.get("grain"),
        enums=enums,
        sources=sources,
        joins=joins,
        targets=targets,
    )


def parse_mapper_yaml(path: str | Path) -> MapperConfig:
    """Parse a single mapper YAML file into a MapperConfig."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)
    return parse_mapper_dict(data)


def load_all_mappers(mappers_dir: str | Path) -> list[MapperConfig]:
    """Load all mapper YAML files from a directory."""
    mappers_dir = Path(mappers_dir)
    mappers = []
    for path in sorted(mappers_dir.glob("*.yaml")):
        mappers.append(parse_mapper_yaml(path))
    return mappers


def load_mapper_by_name(name: str, mappers_dir: str | Path) -> MapperConfig | None:
    """Load a single mapper by its name."""
    for mapper in load_all_mappers(mappers_dir):
        if mapper.name == name:
            return mapper
    return None


def _validate_filter_leaves(filter_data, path: str, source: dict,
                            errors: list[ValidationError]):
    """Walk a filter recursively and validate each leaf expression."""
    source_cols = set(source.get("columns", []))

    if isinstance(filter_data, str):
        # filter_parse: leaf expression must parse
        try:
            _parse_expression_str(filter_data)
        except (ValueError, Exception) as e:
            errors.append(ValidationError(
                path=path,
                message=f"failed to parse filter expression: {e}",
                rule="filter_parse",
            ))
            return

        # filter_column_ref: columns must exist in this source
        for col_name in collect_filter_column_refs(filter_data):
            if col_name not in source_cols:
                errors.append(ValidationError(
                    path=path,
                    message=f"filter column {col_name!r} not found in source columns",
                    rule="filter_column_ref",
                ))
        return

    if isinstance(filter_data, dict):
        if "and" in filter_data:
            for j, item in enumerate(filter_data["and"]):
                _validate_filter_leaves(item, f"{path}.and[{j}]", source, errors)
        if "or" in filter_data:
            for j, item in enumerate(filter_data["or"]):
                _validate_filter_leaves(item, f"{path}.or[{j}]", source, errors)
        if "not" in filter_data:
            _validate_filter_leaves(filter_data["not"], f"{path}.not", source, errors)


def _check_mapper_rules(data: dict) -> list[ValidationError]:
    """Phase 2: semantic cross-field checks for mapper configs."""
    errors = []

    sources = data.get("sources", [])
    source_aliases = {s["alias"] for s in sources}
    source_columns = {}
    for s in sources:
        source_columns[s["alias"]] = set(s.get("columns", []))

    grain = data.get("grain")
    joins = data.get("joins", [])
    targets = data.get("targets", [])
    enums = data.get("enums") or {}
    has_multi_group = any(tgt.get("rows") for tgt in targets)

    # table_or_union — each source must have exactly one of table or union
    for i, s in enumerate(sources):
        has_table = "table" in s and s["table"] is not None
        has_union = "union" in s and s["union"] is not None
        if has_table and has_union:
            errors.append(ValidationError(
                path=f"sources[{i}]",
                message="source must have 'table' or 'union', not both",
                rule="table_or_union",
            ))
        elif not has_table and not has_union:
            errors.append(ValidationError(
                path=f"sources[{i}]",
                message="source must have either 'table' or 'union'",
                rule="table_or_union",
            ))

    # grain_required — grain must be present unless multi-group targets are used
    if not grain and not has_multi_group:
        errors.append(ValidationError(
            path="grain",
            message="grain is required when targets do not use rows",
            rule="grain_required",
        ))

    # grain_alias_exists
    if grain and not has_multi_group and grain not in source_aliases:
        errors.append(ValidationError(
            path="grain",
            message=f"grain alias {grain!r} not found in sources",
            rule="grain_alias_exists",
        ))

    # unique_source_alias
    seen_aliases = []
    for i, s in enumerate(sources):
        alias = s.get("alias")
        if alias in seen_aliases:
            errors.append(ValidationError(
                path=f"sources[{i}].alias",
                message=f"duplicate source alias {alias!r}",
                rule="unique_source_alias",
            ))
        seen_aliases.append(alias)

    # filter checks
    for i, s in enumerate(sources):
        filt = s.get("filter")
        if filt is None:
            continue
        _validate_filter_leaves(filt, f"sources[{i}].filter", s, errors)

    # deduplicate checks
    for i, s in enumerate(sources):
        dedup = s.get("deduplicate")
        if not dedup:
            continue
        source_cols = set(s.get("columns", []))
        for col_name in dedup:
            if col_name not in source_cols:
                errors.append(ValidationError(
                    path=f"sources[{i}].deduplicate",
                    message=f"deduplicate column {col_name!r} not found in source columns",
                    rule="deduplicate_column_ref",
                ))

    # join checks
    for i, join in enumerate(joins):
        prefix = f"joins[{i}]"
        from_alias = join.get("from")
        to_alias = join.get("to")

        # join_alias_exists
        if from_alias not in source_aliases:
            errors.append(ValidationError(
                path=f"{prefix}.from",
                message=f"join from alias {from_alias!r} not found in sources",
                rule="join_alias_exists",
            ))
        if to_alias not in source_aliases:
            errors.append(ValidationError(
                path=f"{prefix}.to",
                message=f"join to alias {to_alias!r} not found in sources",
                rule="join_alias_exists",
            ))

        for j, cond in enumerate(join.get("on", [])):
            cond_prefix = f"{prefix}.on[{j}]"
            operator = cond.get("operator", "eq")
            for side in ("left", "right"):
                ref = cond.get(side, "")

                # join_column_ref_format
                if "." not in ref:
                    errors.append(ValidationError(
                        path=f"{cond_prefix}.{side}",
                        message=f"join condition {side} {ref!r} must be alias.column format",
                        rule="join_column_ref_format",
                    ))
                    continue

                parts = ref.split(".", 1)
                ref_alias, ref_col = parts[0], parts[1]

                # join_column_ref
                if ref_alias in source_columns and ref_col not in source_columns[ref_alias]:
                    errors.append(ValidationError(
                        path=f"{cond_prefix}.{side}",
                        message=f"column {ref_col!r} not found in source {ref_alias!r}",
                        rule="join_column_ref",
                    ))

            # json_array_contains requires json_path
            if operator == "json_array_contains" and not cond.get("json_path"):
                errors.append(ValidationError(
                    path=f"{cond_prefix}.operator",
                    message="json_array_contains operator requires json_path",
                    rule="json_array_contains_requires_path",
                ))

            # json_path only valid with json_array_contains
            if cond.get("json_path") and operator != "json_array_contains":
                errors.append(ValidationError(
                    path=f"{cond_prefix}.json_path",
                    message="json_path is only valid with json_array_contains operator",
                    rule="json_path_requires_operator",
                ))

    # target checks — expression validation only (write strategy resolved from ontology at runtime)
    for i, tgt in enumerate(targets):
        prefix = f"targets[{i}]"

        # target_rows_or_columns — must have columns XOR rows
        has_columns = bool(tgt.get("columns"))
        has_rows = bool(tgt.get("rows"))
        if has_columns and has_rows:
            errors.append(ValidationError(
                path=prefix,
                message="target must have 'columns' or 'rows', not both",
                rule="target_rows_or_columns",
            ))
        elif not has_columns and not has_rows:
            errors.append(ValidationError(
                path=prefix,
                message="target must have either 'columns' or 'rows'",
                rule="target_rows_or_columns",
            ))

        # Row group validation
        for k, rg in enumerate(tgt.get("rows", [])):
            rg_prefix = f"{prefix}.rows[{k}]"
            rg_from = rg.get("from")
            rg_joins = rg.get("joins", [])

            # row_group_alias_exists — from alias must exist in sources
            if rg_from not in source_aliases:
                errors.append(ValidationError(
                    path=f"{rg_prefix}.from",
                    message=f"row group from alias {rg_from!r} not found in sources",
                    rule="row_group_alias_exists",
                ))

            # Valid aliases for this row group = from + all join.to
            rg_valid_aliases = {rg_from}
            for rg_join in rg_joins:
                to_alias = rg_join.get("to")
                if to_alias not in source_aliases:
                    errors.append(ValidationError(
                        path=f"{rg_prefix}.joins",
                        message=f"row group join to alias {to_alias!r} not found in sources",
                        rule="row_group_alias_exists",
                    ))
                else:
                    rg_valid_aliases.add(to_alias)

            # Expression checks for row group columns
            for j, col in enumerate(rg.get("columns", [])):
                col_prefix = f"{rg_prefix}.columns[{j}]"
                expr_str = col.get("expr", "")

                # expression_parse
                try:
                    ast = _parse_expression_str(expr_str)
                except (ValueError, Exception) as e:
                    errors.append(ValidationError(
                        path=f"{col_prefix}.expr",
                        message=f"failed to parse expression: {e}",
                        rule="expression_parse",
                    ))
                    continue

                # expression_alias_ref / expression_column_ref
                for ref in collect_refs(ast):
                    ref_alias, ref_col = ref.parts[0], ref.parts[1]
                    if ref_alias not in rg_valid_aliases:
                        errors.append(ValidationError(
                            path=f"{col_prefix}.expr",
                            message=f"alias {ref_alias!r} not in row group aliases {sorted(rg_valid_aliases)}",
                            rule="expression_alias_ref",
                        ))
                    elif ref_col not in source_columns.get(ref_alias, set()):
                        errors.append(ValidationError(
                            path=f"{col_prefix}.expr",
                            message=f"column {ref_col!r} not found in source {ref_alias!r}",
                            rule="expression_column_ref",
                        ))

                # enum_mapping_ref
                for enum_name in collect_enum_refs(ast):
                    if enum_name not in enums:
                        errors.append(ValidationError(
                            path=f"{col_prefix}.expr",
                            message=f"ENUM mapping {enum_name!r} not found in enums block",
                            rule="enum_mapping_ref",
                        ))

        # Expression checks per column
        for j, col in enumerate(tgt.get("columns", [])):
            col_prefix = f"{prefix}.columns[{j}]"
            expr_str = col.get("expr", "")

            # expression_parse
            try:
                ast = _parse_expression_str(expr_str)
            except (ValueError, Exception) as e:
                errors.append(ValidationError(
                    path=f"{col_prefix}.expr",
                    message=f"failed to parse expression: {e}",
                    rule="expression_parse",
                ))
                continue

            # expression_alias_ref / expression_column_ref
            for ref in collect_refs(ast):
                ref_alias, ref_col = ref.parts[0], ref.parts[1]
                if ref_alias not in source_aliases:
                    errors.append(ValidationError(
                        path=f"{col_prefix}.expr",
                        message=f"alias {ref_alias!r} not found in sources",
                        rule="expression_alias_ref",
                    ))
                elif ref_col not in source_columns.get(ref_alias, set()):
                    errors.append(ValidationError(
                        path=f"{col_prefix}.expr",
                        message=f"column {ref_col!r} not found in source {ref_alias!r}",
                        rule="expression_column_ref",
                    ))

            # enum_mapping_ref
            for enum_name in collect_enum_refs(ast):
                if enum_name not in enums:
                    errors.append(ValidationError(
                        path=f"{col_prefix}.expr",
                        message=f"ENUM mapping {enum_name!r} not found in enums block",
                        rule="enum_mapping_ref",
                    ))

    return errors


def validate_mapper_yaml(path: str | Path) -> list[ValidationError]:
    """Validate a mapper YAML config file (both phases)."""
    path = Path(path)
    with open(path) as f:
        data = yaml.safe_load(f)

    # Phase 1: structural
    errors = validate_schema(data, "mapper")
    if errors:
        return errors

    # Phase 2: semantic (only if structure is valid)
    errors.extend(_check_mapper_rules(data))
    return errors
