# Copyright 2024-2026 Oliver Fenton
# SPDX-License-Identifier: Apache-2.0

"""Database abstraction layer."""

from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path


def _now_utc() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(UTC).isoformat()


class BackendUnavailableError(Exception):
    """Raised when a database backend cannot be reached (connection refused, auth failure, etc.).

    The API layer catches this and returns HTTP 503 Service Unavailable.
    """


class DBBackend(ABC):
    """Abstract database backend."""

    last_columns: list[str] = []  # populated after execute() calls that return rows
    dialect: str = "sqlite"  # overridden by each concrete backend

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def execute(self, sql: str, params: tuple | list = ()) -> list: ...

    @abstractmethod
    def table_exists(self, table: str) -> bool: ...

    @abstractmethod
    def get_columns(self, table: str) -> list[str]: ...

    @abstractmethod
    def create_table(
        self,
        table: str,
        columns: dict[str, str],
        primary_keys: list[str],
        foreign_keys: dict[str, str] | None = None,
        order_by: list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    def add_columns(self, table: str, columns: dict[str, str]) -> None: ...

    @abstractmethod
    def upsert(self, table: str, row: dict) -> None: ...

    @abstractmethod
    def append(self, table: str, row: dict) -> None: ...

    @abstractmethod
    def truncate(self, table: str) -> None: ...

    @abstractmethod
    def ensure_meta_table(self) -> None: ...

    @abstractmethod
    def get_last_run(self, task_name: str, param_key: str | None = None) -> str | None: ...

    @abstractmethod
    def record_run(
        self, task_name: str, started_at: str, completed_at: str,
        record_count: int, status: str, param_key: str | None = None,
    ) -> None: ...


class SQLiteBackend(DBBackend):
    """SQLite implementation with WAL mode and parameterized queries."""

    dialect = "sqlite"

    def __init__(self, db_path: str | Path | None = None):
        """Initialize with a file path, or None for in-memory DB."""
        self.db_path = str(db_path) if db_path else ":memory:"
        self.conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        # timeout=30: retry for 30 s on SQLITE_BUSY before raising OperationalError.
        # Without this, any write contention (e.g. concurrent API request) fails immediately.
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute(self, sql: str, params: tuple | list = ()) -> list:
        assert self.conn, "Not connected"
        cursor = self.conn.execute(sql, params)
        self.conn.commit()
        self.last_columns = [d[0] for d in cursor.description] if cursor.description else []
        return cursor.fetchall()

    def table_exists(self, table: str) -> bool:
        assert self.conn, "Not connected"
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return row is not None

    def get_columns(self, table: str) -> list[str]:
        assert self.conn, "Not connected"
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [r["name"] for r in rows]

    def create_table(
        self,
        table: str,
        columns: dict[str, str],
        primary_keys: list[str],
        foreign_keys: dict[str, str] | None = None,
        order_by: list[str] | None = None,
    ) -> None:
        assert self.conn, "Not connected"
        col_defs = [f"{name} {typ}" for name, typ in columns.items()]
        if primary_keys:
            col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        if foreign_keys:
            for col_name, ref in foreign_keys.items():
                col_defs.append(f"FOREIGN KEY ({col_name}) REFERENCES {ref}")
        sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
        self.conn.execute(sql)
        # Ensure a UNIQUE INDEX exists on the primary key columns — handles tables
        # that predate this constraint (e.g. created by the old SQLAlchemy path).
        # SQLite requires the referenced column to have a UNIQUE or PRIMARY KEY
        # constraint for FK enforcement; without it FK checks raise "mismatch".
        if primary_keys:
            idx_name = f"uix_{table}_pk"
            cols_str = ", ".join(primary_keys)
            self.conn.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {idx_name} ON {table} ({cols_str})"
            )
        self.conn.commit()

    def add_columns(self, table: str, columns: dict[str, str]) -> None:
        assert self.conn, "Not connected"
        existing = set(self.get_columns(table))
        for name, typ in columns.items():
            if name not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
        self.conn.commit()

    def upsert(self, table: str, row: dict) -> None:
        """INSERT OR REPLACE for DIMENSION tables."""
        assert self.conn, "Not connected"
        cols = list(row.keys())
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(cols)
        values = [
            json.dumps(v) if isinstance(v, (dict, list)) else v
            for v in row.values()
        ]
        sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"
        self.conn.execute(sql, values)
        self.conn.commit()

    def append(self, table: str, row: dict) -> None:
        """Plain INSERT for FACT tables, adds inserted_at timestamp."""
        assert self.conn, "Not connected"
        row_with_ts = {**row, "inserted_at": _now_utc()}
        cols = list(row_with_ts.keys())
        placeholders = ", ".join("?" for _ in cols)
        col_names = ", ".join(cols)
        values = [
            json.dumps(v) if isinstance(v, (dict, list)) else v
            for v in row_with_ts.values()
        ]
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        self.conn.execute(sql, values)
        self.conn.commit()

    def truncate(self, table: str) -> None:
        """Delete all rows from a table (SQLite has no TRUNCATE)."""
        assert self.conn, "Not connected"
        self.conn.execute(f"DELETE FROM {table}")
        self.conn.commit()

    def ensure_meta_table(self) -> None:
        """Create the _task_runs metadata table if it doesn't exist."""
        assert self.conn, "Not connected"
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS _task_runs ("
            "  task_name TEXT NOT NULL,"
            "  started_at TEXT NOT NULL,"
            "  completed_at TEXT NOT NULL,"
            "  record_count INTEGER NOT NULL,"
            "  status TEXT NOT NULL,"
            "  param_key TEXT"
            ")"
        )
        self.conn.commit()
        # Migrate existing tables missing the param_key column
        cols = self.get_columns("_task_runs")
        if "param_key" not in cols:
            self.conn.execute("ALTER TABLE _task_runs ADD COLUMN param_key TEXT")
            self.conn.commit()

    def get_last_run(self, task_name: str, param_key: str | None = None) -> str | None:
        """Return the max started_at for successful runs of a task.

        When param_key is provided, returns the last run for that specific
        param_set. When None, returns the last run where param_key IS NULL
        (i.e. non-wildcard task-level runs).
        """
        assert self.conn, "Not connected"
        if param_key is not None:
            row = self.conn.execute(
                "SELECT MAX(started_at) AS last_run FROM _task_runs "
                "WHERE task_name = ? AND param_key = ? AND status = 'success'",
                (task_name, param_key),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT MAX(started_at) AS last_run FROM _task_runs "
                "WHERE task_name = ? AND param_key IS NULL AND status = 'success'",
                (task_name,),
            ).fetchone()
        return row["last_run"] if row else None

    def record_run(
        self, task_name: str, started_at: str, completed_at: str,
        record_count: int, status: str, param_key: str | None = None,
    ) -> None:
        """Insert a run record into _task_runs."""
        assert self.conn, "Not connected"
        self.conn.execute(
            "INSERT INTO _task_runs (task_name, started_at, completed_at, record_count, status, param_key) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (task_name, started_at, completed_at, record_count, status, param_key),
        )
        self.conn.commit()


# ---------------------------------------------------------------------------
# ClickHouseBackend
# ---------------------------------------------------------------------------

# Type aliases for the clickhouse_connect types we use, so the module can be
# imported even when clickhouse-connect is not installed (the backend will
# raise ImportError only at instantiation time).
_CH_CLIENT_TYPE = None


class ClickHouseBackend(DBBackend):
    """ClickHouse implementation using clickhouse-connect (HTTP, port 8123).

    dialect = "clickhouse" so expression features are compiled with ClickHouse
    functions (dateDiff, toDateTime, …) instead of SQLite (julianday, …).

    All entity tables use ReplacingMergeTree with a monotonic ``_version``
    column (epoch milliseconds).  Queries that must see a consistent snapshot
    should use ``SELECT ... FINAL`` — the ``execute()`` method automatically
    appends ``FINAL`` for any ``FROM sem_`` or ``FROM raw_`` table reference so
    callers do not need to handle this themselves.

    ``upsert`` and ``append`` both INSERT rows; deduplication is left to
    ClickHouse's background merge process.  ``_version`` is set to the current
    epoch-ms on every write so a re-run always supersedes earlier rows.
    """

    # ClickHouse SQL type mapping from the generic column-type strings used by
    # the semantic layer (e.g. "TEXT", "INTEGER", "FLOAT", "REAL", "NUMERIC").
    dialect = "clickhouse"

    _TYPE_MAP: dict[str, str] = {
        "text":    "Nullable(String)",
        "string":  "Nullable(String)",
        "varchar": "Nullable(String)",
        "integer": "Nullable(Int64)",
        "int":     "Nullable(Int64)",
        "bigint":  "Nullable(Int64)",
        "float":   "Nullable(Float64)",
        "real":    "Nullable(Float64)",
        "double":  "Nullable(Float64)",
        "numeric": "Nullable(Float64)",
        "boolean": "UInt8",
        "bool":    "UInt8",
    }

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "default",
        username: str = "default",
        password: str = "",
        always_final: bool = False,
        raw_database: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.always_final = always_final
        # When set, _maybe_add_final qualifies raw_ table refs as
        # raw_database.tablename so the sem backend can cross-database query.
        self.raw_database = raw_database
        self._client = None
        self.last_columns: list[str] = []
        # Cache of {table: {col_name: ch_type_str}} — used for value coercion
        # before insert so clickhouse-connect receives the right Python types.
        self._col_type_cache: dict[str, dict[str, str]] = {}

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        try:
            import clickhouse_connect  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "clickhouse-connect is required for ClickHouseBackend. "
                "Install it with: poetry install -E clickhouse"
            ) from exc
        try:
            # Create the target database if it doesn't exist.  Connect to the
            # always-present 'default' database first, issue CREATE DATABASE, then
            # reconnect to the target database.
            init = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )
            init.command(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
            init.close()
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
            )
        except clickhouse_connect.driver.exceptions.ClickHouseError as exc:
            raise BackendUnavailableError(
                f"ClickHouse unavailable at {self.host}:{self.port} "
                f"(database: {self.database}): {exc}"
            ) from exc

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: tuple | list = ()) -> list:
        """Execute SQL and return rows as lists.

        Automatically appends ``FINAL`` to any SELECT that references a
        ``sem_`` or ``raw_`` table without it, so callers get a deduplicated
        view from ReplacingMergeTree without needing to know the engine.

        When ``always_final=True`` (split-database backends), FINAL is injected
        after every FROM/JOIN table reference — all tables are ReplacingMergeTree.
        """
        assert self._client is not None, "Not connected"
        sql = self._maybe_add_final(sql, self.always_final, raw_database=self.raw_database)
        sql, ch_params = self._bind_params(sql, params)
        result = self._client.query(sql, parameters=ch_params)
        self.last_columns = list(result.column_names)
        rows = [list(row) for row in result.result_rows]
        # clickhouse-connect returns no column_names for empty result sets.
        # Recover schema by running LIMIT 0, which always returns column metadata.
        if not self.last_columns and not rows and sql.lstrip().upper().startswith("SELECT"):
            schema = self._client.query(f"SELECT * FROM ({sql}) LIMIT 0", parameters=ch_params)
            self.last_columns = list(schema.column_names)
        return rows

    @staticmethod
    def _bind_params(sql: str, params: tuple | list) -> tuple[str, dict]:
        """Convert positional ? params to clickhouse-connect named {p0:String} params.

        Returns (rewritten_sql, params_dict).  Replaces each ``?`` placeholder
        in order with ``{p0:String}``, ``{p1:String}``, … so that
        clickhouse-connect can substitute them safely.
        """
        if not params:
            return sql, {}
        ch_params: dict = {}
        for idx, p in enumerate(params):
            key = f"p{idx}"
            sql = sql.replace("?", f"{{{key}:String}}", 1)
            ch_params[key] = str(p)
        return sql, ch_params

    @staticmethod
    def _maybe_add_final(sql: str, always_final: bool = False, raw_database: str | None = None) -> str:
        """Inject FINAL into a SELECT after ReplacingMergeTree table references.

        Two modes:

        ``always_final=False`` (default / legacy):
            Injects FINAL after every ``FROM`` or ``JOIN`` reference to a
            ``sem_``- or ``raw_``-prefixed table.  Used when sem and raw tables
            share a single ClickHouse database and are distinguished by prefix.

        ``always_final=True`` (split-database mode):
            Injects FINAL after every ``FROM`` or ``JOIN`` table reference in
            the query.  Used when the backend is connected to a dedicated
            ``openflights_raw`` or ``openflights_sem`` database where *all*
            tables are ReplacingMergeTree and the prefix is no longer needed as
            a discriminator.

        Skips injection when FINAL is already present anywhere in the SQL to
        avoid double-injection.  Does not modify non-SELECT statements (INSERT,
        CREATE, etc.).
        """
        import re  # noqa: PLC0415
        upper = sql.upper()
        if "SELECT" not in upper:
            return sql
        if "FINAL" in upper:
            return sql

        # Alias pattern — matches "AS alias" or a bare alias word, but excludes
        # SQL keywords that can follow a table reference.  This handles both
        # `FROM table AS t` (AS-prefixed alias) and `FROM table t` (bare alias,
        # as emitted by the expression compiler).
        _KW = (
            r'FINAL|ON|WHERE|GROUP|HAVING|ORDER|LIMIT|UNION|EXCEPT|INTERSECT|'
            r'LEFT|RIGHT|INNER|FULL|CROSS|SEMI|ANTI|ASOF|JOIN|GLOBAL|LOCAL|'
            r'ANY|ALL|ARRAY|PASTE|PREWHERE|WINDOW|QUALIFY|SETTINGS|FORMAT|SAMPLE'
        )
        _ALIAS = rf'(?:\s+AS\s+\w+|\s+(?!(?:{_KW})\b)\w+)?'

        if always_final:
            # Strip legacy prefixes and optionally qualify raw_ tables with their
            # database name so the sem backend can cross-database query raw tables.
            if raw_database:
                # sem backend: raw_ tables live in a sibling database — qualify
                # them as raw_database.tablename; strip sem_ prefix (local db).
                sql = re.sub(r'\b(sem_)(\w+)', r'\2', sql, flags=re.IGNORECASE)
                sql = re.sub(r'\b(raw_)(\w+)', rf'{raw_database}.\2', sql, flags=re.IGNORECASE)
            else:
                # raw backend or single-database: just strip both prefixes.
                sql = re.sub(r'\b(sem_|raw_)(\w+)', r'\2', sql, flags=re.IGNORECASE)
            # All tables in dedicated sem/raw databases are RMT — apply FINAL to
            # every FROM/JOIN table reference.  Use [\w.]+ to match db.table refs.
            # Subquery parentheses are safely left untouched (no \w match).
            sql = re.sub(
                rf'(\b(?:FROM|JOIN)\s+[\w.]+{_ALIAS})',
                r'\1 FINAL',
                sql,
                flags=re.IGNORECASE,
            )
        else:
            # Legacy mode: only sem_/raw_-prefixed tables are RMT.  Inject
            # FINAL after every matching FROM or JOIN reference (not just the
            # first — a query may join multiple sem_ tables).
            sql = re.sub(
                rf'(\b(?:FROM|JOIN)\s+(?:sem_|raw_)\w+{_ALIAS})',
                r'\1 FINAL',
                sql,
                flags=re.IGNORECASE,
            )
        return sql

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def table_exists(self, table: str) -> bool:
        assert self._client is not None, "Not connected"
        result = self._client.query(
            "SELECT 1 FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": self.database, "tbl": table},
        )
        return len(result.result_rows) > 0

    def get_columns(self, table: str) -> list[str]:
        assert self._client is not None, "Not connected"
        result = self._client.query(
            "SELECT name FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} ORDER BY position",
            parameters={"db": self.database, "tbl": table},
        )
        return [row[0] for row in result.result_rows]

    def _get_col_types(self, table: str) -> dict[str, str]:
        """Return {col_name: ch_type} for *table*, querying ClickHouse if not cached."""
        if table not in self._col_type_cache:
            assert self._client is not None, "Not connected"
            result = self._client.query(
                "SELECT name, type FROM system.columns "
                "WHERE database = {db:String} AND table = {tbl:String}",
                parameters={"db": self.database, "tbl": table},
            )
            self._col_type_cache[table] = {row[0]: row[1] for row in result.result_rows}
        return self._col_type_cache[table]

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def _ch_type(self, sql_type: str) -> str:
        """Map a generic SQL type string to a ClickHouse type."""
        return self._TYPE_MAP.get(sql_type.lower().split("(")[0], "String")

    def create_table(
        self,
        table: str,
        columns: dict[str, str],
        primary_keys: list[str],
        foreign_keys: dict[str, str] | None = None,
        order_by: list[str] | None = None,
    ) -> None:
        assert self._client is not None, "Not connected"
        # ORDER BY columns must be non-Nullable; resolve them before building col_defs.
        # For SCD2 entities, order_by = [entity_key, valid_from] so that re-inserting
        # a closed row (same composite key, higher _version) supersedes the open one.
        order_by_cols = order_by or (list(primary_keys) if primary_keys else list(columns)[:1])
        order_by_set = set(order_by_cols)
        col_defs = []
        for name, typ in columns.items():
            ch_type = self._ch_type(typ)
            if name in order_by_set:
                # Strip Nullable wrapper — ClickHouse rejects Nullable ORDER BY keys
                # unless allow_nullable_key is enabled (disabled by default)
                ch_type = ch_type.removeprefix("Nullable(").removesuffix(")")
            col_defs.append(f"`{name}` {ch_type}")
        # Always add versioning columns for ReplacingMergeTree
        col_defs.append("`_version` UInt64")
        col_defs.append("`_loaded_at` DateTime DEFAULT now()")
        order_cols = ", ".join(f"`{k}`" for k in order_by_cols)
        sql = (
            f"CREATE TABLE IF NOT EXISTS `{table}` "
            f"({', '.join(col_defs)}) "
            f"ENGINE = ReplacingMergeTree(`_version`) "
            f"ORDER BY ({order_cols})"
        )
        self._client.command(sql)
        # Populate type cache for coercion in _prepare_row
        self._col_type_cache[table] = {name: self._ch_type(typ) for name, typ in columns.items()}
        self._col_type_cache[table]["_version"] = "UInt64"
        self._col_type_cache[table]["_loaded_at"] = "DateTime"

    def add_columns(self, table: str, columns: dict[str, str]) -> None:
        assert self._client is not None, "Not connected"
        for name, typ in columns.items():
            self._client.command(
                f"ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS `{name}` {self._ch_type(typ)}"
            )
        # Update cache with the new columns
        self._col_type_cache.setdefault(table, {}).update(
            {name: self._ch_type(typ) for name, typ in columns.items()}
        )

    # ------------------------------------------------------------------
    # DML
    # ------------------------------------------------------------------

    def _version_now(self) -> int:
        """Monotonic version: epoch milliseconds."""
        from datetime import UTC, datetime  # noqa: PLC0415
        return int(datetime.now(UTC).timestamp() * 1000)

    def _prepare_row(self, row: dict, with_inserted_at: bool = False, table: str | None = None) -> dict:
        """Serialise complex values, coerce types against schema, and inject _version.

        When *table* is supplied the column-type cache is consulted so that
        Python values are coerced to the types clickhouse-connect expects:
        - dict/list → JSON string
        - non-None value destined for a String column → str(v)
        This prevents ``TypeError: object of type 'int' has no len()`` that
        clickhouse-connect raises when it receives a Python int for a String column.
        """
        col_types = self._get_col_types(table) if table else {}
        out = {}
        for k, v in row.items():
            ch_type = col_types.get(k, "")
            if isinstance(v, (dict, list)):
                out[k] = json.dumps(v)
            elif v is None and ch_type and "Nullable" not in ch_type:
                # Non-Nullable column — substitute a type-appropriate default so
                # clickhouse-connect doesn't receive None for a required field.
                if "Int" in ch_type or "Float" in ch_type or "UInt" in ch_type:
                    out[k] = 0
                else:
                    out[k] = ""
            elif v is not None and "String" in ch_type:
                out[k] = str(v)
            else:
                out[k] = v
        if with_inserted_at and "inserted_at" not in out:
            out["inserted_at"] = _now_utc()
        out["_version"] = self._version_now()
        return out

    def upsert(self, table: str, row: dict) -> None:
        """INSERT row — ReplacingMergeTree deduplicates on next merge."""
        assert self._client is not None, "Not connected"
        prepared = self._prepare_row(row, table=table)
        self._client.insert(table, [list(prepared.values())], column_names=list(prepared.keys()))

    def bulk_upsert(self, table: str, rows: list[dict]) -> None:
        """INSERT many rows in a single call — much faster than repeated upsert()."""
        if not rows:
            return
        assert self._client is not None, "Not connected"
        col_types = self._get_col_types(table)
        version = self._version_now()
        # Prepare all rows; use the first row's keys for column names
        col_names: list[str] | None = None
        data: list[list] = []
        for row in rows:
            out = {}
            for k, v in row.items():
                ch_type = col_types.get(k, "")
                if isinstance(v, (dict, list)):
                    out[k] = json.dumps(v)
                elif v is None and ch_type and "Nullable" not in ch_type:
                    out[k] = 0 if ("Int" in ch_type or "Float" in ch_type or "UInt" in ch_type) else ""
                elif v is not None and "String" in ch_type:
                    out[k] = str(v)
                else:
                    out[k] = v
            out["_version"] = version
            if col_names is None:
                col_names = list(out.keys())
            data.append([out[c] for c in col_names])
        self._client.insert(table, data, column_names=col_names)

    def bulk_scd2(self, table: str, candidates: list[dict], ctx) -> tuple[int, int]:
        """Write SCD2 history rows via insert-to-close.

        Reads all current open rows (valid_to = '' or NULL) for the entity,
        diffs against *candidates*, then issues a single bulk INSERT containing:
        - A "close" copy of each changed row (same (pk, valid_from) ORDER BY key,
          valid_to set to now; higher _version supersedes the open row via
          ReplacingMergeTree deduplication on next FINAL / background merge).
        - A new open row for each new or changed key.

        Returns (inserted_new, inserted_scd2) counts.
        """
        if not candidates:
            return 0, 0
        assert self._client is not None, "Not connected"

        key_col = ctx.key_col
        tracked_cols = ctx.tracked_cols or []

        # Read all current open rows (FINAL for consistent view)
        result = self._client.query(
            f"SELECT * FROM `{table}` FINAL WHERE valid_to = '' OR valid_to IS NULL"
        )
        col_names = list(result.column_names)
        existing: dict = {}  # key_value -> row_dict
        for row in result.result_rows:
            row_dict = dict(zip(col_names, row))
            existing[row_dict.get(key_col)] = row_dict

        now = _now_utc()
        rows_to_insert: list[dict] = []
        inserted_new = 0
        inserted_scd2 = 0

        for candidate in candidates:
            key_value = candidate.get(key_col)
            current = existing.get(key_value)

            if current is None:
                # New entity — insert open row
                new_row = {k: v for k, v in candidate.items() if k not in ctx.managed_cols}
                new_row["valid_from"] = ctx.initial_valid_from or now
                new_row["valid_to"] = ""
                rows_to_insert.append(new_row)
                inserted_new += 1
            else:
                changed = any(
                    str(current.get(col, "")) != str(candidate.get(col, ""))
                    for col in tracked_cols
                )
                if not changed:
                    continue
                # Close the existing open row by re-inserting it with valid_to set.
                # ReplacingMergeTree keeps the highest _version per (key, valid_from),
                # so this closed re-insert supersedes the still-open one after merge.
                close_row = {
                    k: v for k, v in current.items()
                    if k not in ("_version", "_loaded_at")
                }
                close_row["valid_to"] = now
                rows_to_insert.append(close_row)
                # Insert new open row with current candidate values
                new_row = {k: v for k, v in candidate.items() if k not in ctx.managed_cols}
                new_row["valid_from"] = now
                new_row["valid_to"] = ""
                rows_to_insert.append(new_row)
                inserted_scd2 += 1

        if rows_to_insert:
            self.bulk_upsert(table, rows_to_insert)
        return inserted_new, inserted_scd2

    def append(self, table: str, row: dict) -> None:
        """INSERT row for FACT tables; adds inserted_at timestamp."""
        assert self._client is not None, "Not connected"
        prepared = self._prepare_row(row, with_inserted_at=True, table=table)
        self._client.insert(table, [list(prepared.values())], column_names=list(prepared.keys()))

    def truncate(self, table: str) -> None:
        """Remove all rows from a ReplacingMergeTree table synchronously."""
        assert self._client is not None, "Not connected"
        self._client.command(f"TRUNCATE TABLE `{table}`")

    # ------------------------------------------------------------------
    # Pipeline run tracking
    # ------------------------------------------------------------------

    def ensure_meta_table(self) -> None:
        """Create _task_runs if it doesn't exist."""
        assert self._client is not None, "Not connected"
        self._client.command(
            "CREATE TABLE IF NOT EXISTS `_task_runs` ("
            "  `task_name`    String,"
            "  `started_at`   String,"
            "  `completed_at` String,"
            "  `record_count` Int64,"
            "  `status`       String,"
            "  `param_key`    Nullable(String),"
            "  `_version`     UInt64,"
            "  `_loaded_at`   DateTime DEFAULT now()"
            ") ENGINE = ReplacingMergeTree(`_version`)"
            "  ORDER BY (task_name, started_at, assumeNotNull(param_key))"
        )

    def get_last_run(self, task_name: str, param_key: str | None = None) -> str | None:
        assert self._client is not None, "Not connected"
        if param_key is not None:
            result = self._client.query(
                "SELECT max(started_at) FROM `_task_runs` FINAL "
                "WHERE task_name = {tn:String} AND param_key = {pk:String} AND status = 'success'",
                parameters={"tn": task_name, "pk": param_key},
            )
        else:
            result = self._client.query(
                "SELECT max(started_at) FROM `_task_runs` FINAL "
                "WHERE task_name = {tn:String} AND param_key IS NULL AND status = 'success'",
                parameters={"tn": task_name},
            )
        rows = result.result_rows
        val = rows[0][0] if rows else None
        return val if val else None

    def record_run(
        self,
        task_name: str,
        started_at: str,
        completed_at: str,
        record_count: int,
        status: str,
        param_key: str | None = None,
    ) -> None:
        assert self._client is not None, "Not connected"
        row = {
            "task_name": task_name,
            "started_at": started_at,
            "completed_at": completed_at,
            "record_count": record_count,
            "status": status,
            "param_key": param_key,
        }
        prepared = self._prepare_row(row)
        self._client.insert(
            "_task_runs",
            [list(prepared.values())],
            column_names=list(prepared.keys()),
        )


# ---------------------------------------------------------------------------
# Backend factory
# ---------------------------------------------------------------------------

def get_backend(model_dir: Path, namespace: str = "sem") -> DBBackend:
    """Return the appropriate DBBackend for the given model directory.

    Reads ``database:`` from ``config.yaml``; defaults to SQLite.
    Production models always specify their backend explicitly.

    ``namespace`` is only meaningful for ClickHouse backends:
      ``"sem"``  — connect to the entity database, same name as ``database`` config
                   (e.g. ``openflights``)
      ``"raw"``  — connect to the extractor database, e.g. ``openflights_raw``

    Both ClickHouse namespaces get ``always_final=True`` since every table in
    a dedicated sem/raw database is a ReplacingMergeTree.

    """
    from core.model_config import load_database_config  # noqa: PLC0415
    cfg = load_database_config(model_dir)
    backend = cfg["backend"]

    if backend == "sqlite":
        db_path = cfg.get("path", "data/data.db")
        return SQLiteBackend(db_path=Path(model_dir) / db_path)

    if backend == "clickhouse":
        base_db = cfg.get("database", model_dir.name)
        raw_db = cfg.get("raw_database", f"{base_db}_raw")
        if namespace == "raw":
            database = raw_db
            raw_database = None  # raw backend doesn't need cross-db refs
        else:
            database = cfg.get("sem_database", base_db)
            raw_database = raw_db  # sem backend can cross-query raw tables
        return ClickHouseBackend(
            host=cfg.get("host", "localhost"),
            port=int(cfg.get("port", 8123)),
            database=database,
            username=cfg.get("username", "default"),
            password=cfg.get("password") or "",
            always_final=True,
            raw_database=raw_database,
        )

    raise ValueError(f"Unknown database backend {backend!r} in {model_dir}/config.yaml")
