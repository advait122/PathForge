from __future__ import annotations

import contextvars
import logging
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

from backend.roadmap_engine.config import DATABASE_URL, DB_PATH

logger = logging.getLogger(__name__)
_QUERY_COUNT: contextvars.ContextVar[int] = contextvars.ContextVar("db_query_count", default=0)


def _query_timing_enabled() -> bool:
    raw = os.getenv("DB_QUERY_TIMING_LOG", "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def reset_query_count() -> None:
    _QUERY_COUNT.set(0)


def get_query_count() -> int:
    return int(_QUERY_COUNT.get())


def _increment_query_count() -> None:
    _QUERY_COUNT.set(get_query_count() + 1)


def _compact_query_for_log(query: str, limit: int = 180) -> str:
    compact = " ".join(str(query).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _log_query_start(query: str) -> None:
    if _query_timing_enabled():
        text = f"[perf][db][start] sql={_compact_query_for_log(query)}"
        print(text)
        logger.info(text)


def _log_query_end(query: str, duration_s: float, error: Exception | None = None) -> None:
    if not _query_timing_enabled():
        return
    if error is None:
        text = f"[perf][db][done] {duration_s:.4f}s sql={_compact_query_for_log(query)}"
        print(text)
        logger.info(text)
    else:
        text = (
            f"[perf][db][error] {duration_s:.4f}s "
            f"sql={_compact_query_for_log(query)} err={error}"
        )
        print(text)
        logger.info(text)


def is_postgres_enabled() -> bool:
    return bool(DATABASE_URL)


class CompatRow(dict):
    """Row object compatible with sqlite3.Row-style access."""

    def __init__(self, columns: Sequence[str], values: Sequence[Any]):
        super().__init__(zip(columns, values))
        self._values = tuple(values)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)

    def __iter__(self):
        return iter(self._values)


def _strip_trailing_semicolon(query: str) -> tuple[str, bool]:
    stripped = query.rstrip()
    if stripped.endswith(";"):
        return stripped[:-1].rstrip(), True
    return stripped, False


def _replace_qmark_placeholders(query: str) -> str:
    converted: list[str] = []
    in_single_quote = False
    i = 0
    while i < len(query):
        char = query[i]
        if char == "'":
            converted.append(char)
            if in_single_quote and i + 1 < len(query) and query[i + 1] == "'":
                converted.append(query[i + 1])
                i += 1
            else:
                in_single_quote = not in_single_quote
        elif char == "?" and not in_single_quote:
            converted.append("%s")
        else:
            converted.append(char)
        i += 1
    return "".join(converted)


def _translate_sql_for_postgres(query: str) -> str:
    translated = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "BIGSERIAL PRIMARY KEY",
        query,
        flags=re.IGNORECASE,
    )

    if re.search(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", translated, flags=re.IGNORECASE):
        translated = re.sub(
            r"\bINSERT\s+OR\s+IGNORE\s+INTO\b",
            "INSERT INTO",
            translated,
            count=1,
            flags=re.IGNORECASE,
        )
        if not re.search(r"\bON\s+CONFLICT\b", translated, flags=re.IGNORECASE):
            without_semicolon, has_semicolon = _strip_trailing_semicolon(translated)
            translated = f"{without_semicolon} ON CONFLICT DO NOTHING"
            if has_semicolon:
                translated += ";"

    return _replace_qmark_placeholders(translated)


def _is_insert_statement(query: str) -> bool:
    return bool(re.match(r"^\s*INSERT\b", query, flags=re.IGNORECASE))


def _append_returning_id_if_needed(query: str) -> tuple[str, bool]:
    if not _is_insert_statement(query):
        return query, False
    if re.search(r"\bRETURNING\b", query, flags=re.IGNORECASE):
        return query, False

    without_semicolon, has_semicolon = _strip_trailing_semicolon(query)
    query_with_returning = f"{without_semicolon} RETURNING id"
    if has_semicolon:
        query_with_returning += ";"
    return query_with_returning, True


class PostgresCompatCursor:
    def __init__(self, raw_cursor: Any):
        self._cursor = raw_cursor
        self._prefetched_rows: list[tuple[Any, ...]] = []
        self.lastrowid: int | None = None

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def description(self) -> Any:
        return self._cursor.description

    def _adapt_row(self, row: Any) -> CompatRow | None:
        if row is None:
            return None
        columns = [column[0] for column in (self._cursor.description or [])]
        return CompatRow(columns, row)

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> "PostgresCompatCursor":
        translated_query = _translate_sql_for_postgres(query)
        translated_query, has_returning_id = _append_returning_id_if_needed(translated_query)
        _increment_query_count()
        _log_query_start(translated_query)
        started = time.perf_counter()

        self.lastrowid = None
        self._prefetched_rows.clear()

        try:
            if parameters is None:
                self._cursor.execute(translated_query)
            else:
                self._cursor.execute(translated_query, tuple(parameters))

            if has_returning_id:
                inserted_row = self._cursor.fetchone()
                if inserted_row is not None:
                    self._prefetched_rows.append(inserted_row)
                    if inserted_row[0] is not None:
                        self.lastrowid = int(inserted_row[0])
        except Exception as error:
            _log_query_end(translated_query, time.perf_counter() - started, error)
            raise

        _log_query_end(translated_query, time.perf_counter() - started)

        return self

    def executemany(
        self,
        query: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> "PostgresCompatCursor":
        translated_query = _translate_sql_for_postgres(query)
        normalized_parameters = [
            tuple(parameter_set) for parameter_set in seq_of_parameters
        ]
        _increment_query_count()
        _log_query_start(translated_query)
        started = time.perf_counter()
        try:
            self._cursor.executemany(translated_query, normalized_parameters)
        except Exception as error:
            _log_query_end(translated_query, time.perf_counter() - started, error)
            raise
        _log_query_end(translated_query, time.perf_counter() - started)
        self.lastrowid = None
        self._prefetched_rows.clear()
        return self

    def fetchone(self) -> CompatRow | None:
        if self._prefetched_rows:
            raw_row = self._prefetched_rows.pop(0)
        else:
            raw_row = self._cursor.fetchone()
        return self._adapt_row(raw_row)

    def fetchall(self) -> list[CompatRow]:
        raw_rows: list[Any] = []
        if self._prefetched_rows:
            raw_rows.extend(self._prefetched_rows)
            self._prefetched_rows.clear()
        raw_rows.extend(self._cursor.fetchall())
        return [self._adapt_row(row) for row in raw_rows if row is not None]

    def close(self) -> None:
        self._cursor.close()


class PostgresCompatConnection:
    def __init__(self, raw_connection: Any):
        self._connection = raw_connection

    def cursor(self) -> PostgresCompatCursor:
        return PostgresCompatCursor(self._connection.cursor())

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> PostgresCompatCursor:
        cursor = self.cursor()
        cursor.execute(query, parameters)
        return cursor

    def executemany(
        self,
        query: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> PostgresCompatCursor:
        cursor = self.cursor()
        cursor.executemany(query, seq_of_parameters)
        return cursor

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class SqliteCompatCursor:
    def __init__(self, raw_cursor: sqlite3.Cursor):
        self._cursor = raw_cursor

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def lastrowid(self) -> Any:
        return self._cursor.lastrowid

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> "SqliteCompatCursor":
        _increment_query_count()
        _log_query_start(query)
        started = time.perf_counter()
        try:
            if parameters is None:
                self._cursor.execute(query)
            else:
                self._cursor.execute(query, tuple(parameters))
        except Exception as error:
            _log_query_end(query, time.perf_counter() - started, error)
            raise
        _log_query_end(query, time.perf_counter() - started)
        return self

    def executemany(
        self,
        query: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> "SqliteCompatCursor":
        _increment_query_count()
        _log_query_start(query)
        started = time.perf_counter()
        normalized_parameters = [tuple(parameter_set) for parameter_set in seq_of_parameters]
        try:
            self._cursor.executemany(query, normalized_parameters)
        except Exception as error:
            _log_query_end(query, time.perf_counter() - started, error)
            raise
        _log_query_end(query, time.perf_counter() - started)
        return self

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()

    def close(self) -> None:
        self._cursor.close()


class SqliteCompatConnection:
    def __init__(self, raw_connection: sqlite3.Connection):
        self._connection = raw_connection

    def cursor(self) -> SqliteCompatCursor:
        return SqliteCompatCursor(self._connection.cursor())

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> SqliteCompatCursor:
        cursor = self.cursor()
        cursor.execute(query, parameters)
        return cursor

    def executemany(
        self,
        query: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> SqliteCompatCursor:
        cursor = self.cursor()
        cursor.executemany(query, seq_of_parameters)
        return cursor

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


def _connect_postgres() -> PostgresCompatConnection:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "DATABASE_URL is set but psycopg2 is not installed. "
            "Install psycopg2-binary before starting the app."
        ) from exc

    try:
        raw_connection = psycopg2.connect(DATABASE_URL)
    except Exception as exc:
        raise RuntimeError(
            "Failed to connect to PostgreSQL using DATABASE_URL."
        ) from exc
    return PostgresCompatConnection(raw_connection)


def get_connection() -> Any:
    if is_postgres_enabled():
        return _connect_postgres()

    raw_connection = sqlite3.connect(DB_PATH)
    raw_connection.row_factory = sqlite3.Row
    raw_connection.execute("PRAGMA foreign_keys = ON;")
    return SqliteCompatConnection(raw_connection)


@contextmanager
def transaction() -> Iterator[Any]:
    connection = get_connection()
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
