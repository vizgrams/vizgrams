# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Tests for semantic.materialize — backend-agnostic table generation."""

from semantic.materialize import (
    _entity_table_specs,
    materialize_with_backend,
)
from semantic.types import (
    AttributeDef,
    ColumnType,
    EntityDef,
    EventDef,
    HistoryDef,
    HistoryType,
    SemanticHint,
)


def _make_entity(name="TestObj", identity=None, attributes=None, history=None, events=None):
    if identity is None:
        identity = [
            AttributeDef("test_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
        ]
    if attributes is None:
        attributes = [
            AttributeDef("name", ColumnType.STRING, SemanticHint.IDENTIFIER),
        ]
    return EntityDef(
        name=name,
        identity=identity,
        attributes=attributes,
        history=history,
        events=events or [],
    )


def _make_parent_child():
    """Create a parent and child entity with a FK relationship."""
    parent = _make_entity("Parent")
    child = EntityDef(
        name="Child",
        identity=[
            AttributeDef("child_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY),
            AttributeDef("parent_key", ColumnType.STRING, SemanticHint.RELATION, references="Parent"),
        ],
    )
    return parent, child


# ---------------------------------------------------------------------------
# materialize_with_backend — backend-agnostic path (no SQLAlchemy)
# ---------------------------------------------------------------------------

class _StubBackend:
    """Minimal DBBackend stub for testing materialize_with_backend."""

    def __init__(self):
        self.tables: dict[str, dict[str, str]] = {}  # table_name → columns
        self.primary_keys: dict[str, list[str]] = {}  # table_name → pk list
        self.foreign_keys: dict[str, dict[str, str]] = {}  # table_name → fk dict

    def create_table(
        self,
        table: str,
        columns: dict,
        primary_keys: list,
        foreign_keys: dict | None = None,
        order_by: list | None = None,
    ) -> None:
        if table not in self.tables:
            self.tables[table] = dict(columns)
        self.primary_keys[table] = list(primary_keys)
        self.foreign_keys[table] = dict(foreign_keys or {})

    def add_columns(self, table: str, columns: dict) -> None:
        for name, typ in columns.items():
            self.tables[table].setdefault(name, typ)

    def get_columns(self, table: str) -> list[str]:
        return list(self.tables.get(table, {}).keys())

    def table_exists(self, table: str) -> bool:
        return table in self.tables


class TestMaterializeWithBackend:
    def test_creates_base_table(self):
        entity = _make_entity("Widget")
        backend = _StubBackend()
        result = materialize_with_backend([entity], backend)
        assert "widget" in result
        assert "widget" in backend.tables

    def test_base_table_has_columns(self):
        entity = _make_entity(
            "Widget",
            identity=[AttributeDef("widget_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            attributes=[AttributeDef("score", ColumnType.FLOAT, SemanticHint.MEASURE)],
        )
        backend = _StubBackend()
        materialize_with_backend([entity], backend)
        cols = backend.get_columns("widget")
        assert "widget_key" in cols
        assert "score" in cols

    def test_creates_event_table(self):
        event = EventDef(name="StatusChange", attributes=[
            AttributeDef("status", ColumnType.STRING, SemanticHint.IDENTIFIER),
        ])
        entity = _make_entity("Widget", events=[event])
        backend = _StubBackend()
        materialize_with_backend([entity], backend)
        assert "widget_StatusChange_event" in backend.tables

    def test_multiple_entities(self):
        e1 = _make_entity("Alpha")
        e2 = _make_entity("Beta")
        backend = _StubBackend()
        result = materialize_with_backend([e1, e2], backend)
        assert "alpha" in result
        assert "beta" in result

    def test_idempotent_second_call(self):
        entity = _make_entity("Widget")
        backend = _StubBackend()
        materialize_with_backend([entity], backend)
        materialize_with_backend([entity], backend)  # should not raise
        assert "widget" in backend.tables

    def test_adds_new_column_on_second_call(self):
        entity = _make_entity(
            "Widget",
            attributes=[AttributeDef("name", ColumnType.STRING, SemanticHint.IDENTIFIER)],
        )
        backend = _StubBackend()
        materialize_with_backend([entity], backend)

        # Simulate adding a new attribute
        entity2 = _make_entity(
            "Widget",
            attributes=[
                AttributeDef("name", ColumnType.STRING, SemanticHint.IDENTIFIER),
                AttributeDef("score", ColumnType.FLOAT, SemanticHint.MEASURE),
            ],
        )
        materialize_with_backend([entity2], backend)
        cols = backend.get_columns("widget")
        assert "score" in cols

    def test_type_mapping(self):
        entity = _make_entity(
            "Widget",
            identity=[AttributeDef("id", ColumnType.INTEGER, SemanticHint.PRIMARY_KEY)],
            attributes=[
                AttributeDef("label", ColumnType.STRING, SemanticHint.IDENTIFIER),
                AttributeDef("amount", ColumnType.FLOAT, SemanticHint.MEASURE),
            ],
        )
        backend = _StubBackend()
        materialize_with_backend([entity], backend)
        cols = backend.tables["widget"]
        assert cols["id"] == "INTEGER"
        assert cols["label"] == "TEXT"
        assert cols["amount"] == "FLOAT"

    def test_upsert_entity_gets_pk_constraint(self):
        """UPSERT entities (no history) should have their PK in primary_keys."""
        entity = _make_entity(
            "Team",
            identity=[AttributeDef("team_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        )
        backend = _StubBackend()
        materialize_with_backend([entity], backend)
        assert backend.primary_keys["team"] == ["team_key"]

    def test_scd2_entity_gets_no_pk_constraint(self):
        """SCD2 entities must not have a SQL PK constraint (multiple rows per key)."""
        entity = _make_entity(
            "Person",
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        backend = _StubBackend()
        materialize_with_backend([entity], backend)
        assert backend.primary_keys["person"] == []

    def test_fk_generated_for_upsert_target(self):
        """FK is generated when the referenced entity is a UPSERT entity."""
        team = _make_entity(
            "Team",
            identity=[AttributeDef("team_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        )
        person = _make_entity(
            "Person",
            attributes=[
                AttributeDef("name", ColumnType.STRING, SemanticHint.IDENTIFIER),
                AttributeDef("team_key", ColumnType.STRING, SemanticHint.RELATION, references="Team"),
            ],
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        backend = _StubBackend()
        materialize_with_backend([team, person], backend)
        fks = backend.foreign_keys["person"]
        assert "team_key" in fks
        assert fks["team_key"] == "team(team_key)"

    def test_no_fk_for_scd2_target(self):
        """FK is NOT generated when the referenced entity is SCD2 (non-unique PK)."""
        domain = _make_entity(
            "Domain",
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        product = _make_entity(
            "Product",
            attributes=[
                AttributeDef("name", ColumnType.STRING, SemanticHint.IDENTIFIER),
                AttributeDef("domain_key", ColumnType.STRING, SemanticHint.RELATION, references="Domain"),
            ],
        )
        backend = _StubBackend()
        materialize_with_backend([domain, product], backend)
        fks = backend.foreign_keys["product"]
        assert "domain_key" not in fks


# ---------------------------------------------------------------------------
# _entity_table_specs — primary_key / FK logic
# ---------------------------------------------------------------------------

class TestEntityTableSpecs:
    def test_upsert_entity_primary_keys(self):
        entity = _make_entity(
            "Team",
            identity=[AttributeDef("team_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        )
        specs = _entity_table_specs(entity)
        _, _, pks, _, _ = specs[0]
        assert pks == ["team_key"]

    def test_scd2_entity_no_primary_keys(self):
        entity = _make_entity(
            "Person",
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        specs = _entity_table_specs(entity)
        _, _, pks, _, _ = specs[0]
        assert pks == []

    def test_fk_only_for_upsert_target(self):
        team = _make_entity(
            "Team",
            identity=[AttributeDef("team_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
        )
        domain = _make_entity(
            "Domain",
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        person = _make_entity(
            "Person",
            attributes=[
                AttributeDef("team_key", ColumnType.STRING, SemanticHint.RELATION, references="Team"),
                AttributeDef("domain_key", ColumnType.STRING, SemanticHint.RELATION, references="Domain"),
            ],
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        specs = _entity_table_specs(person, all_entities=[team, domain, person])
        _, _, _, fks, _ = specs[0]
        assert "team_key" in fks        # UPSERT target → FK generated
        assert "domain_key" not in fks  # SCD2 target → no FK

    def test_event_table_no_pk_no_fk(self):
        from semantic.types import EventDef
        event = EventDef(
            name="changed",
            attributes=[AttributeDef("state", ColumnType.STRING, SemanticHint.STATE)],
        )
        entity = _make_entity("Widget", events=[event])
        specs = _entity_table_specs(entity)
        assert len(specs) == 2
        _, _, evt_pks, evt_fks, _ = specs[1]
        assert evt_pks == []
        assert evt_fks == {}

    def test_scd2_entity_order_by(self):
        """SCD2 entities get order_by=[pk, valid_from] for ClickHouse composite key."""
        entity = _make_entity(
            "Domain",
            identity=[AttributeDef("domain_key", ColumnType.STRING, SemanticHint.PRIMARY_KEY)],
            history=HistoryDef(
                history_type=HistoryType.SCD2,
                columns=[
                    AttributeDef("valid_from", ColumnType.STRING, SemanticHint.SCD_FROM),
                    AttributeDef("valid_to", ColumnType.STRING, SemanticHint.SCD_TO),
                ],
            ),
        )
        specs = _entity_table_specs(entity)
        _, _, _, _, order_by = specs[0]
        assert order_by == ["domain_key", "valid_from"]

    def test_upsert_entity_order_by_is_none(self):
        """Non-SCD2 entities have order_by=None (backend picks its own strategy)."""
        entity = _make_entity("Widget")
        specs = _entity_table_specs(entity)
        _, _, _, _, order_by = specs[0]
        assert order_by is None
