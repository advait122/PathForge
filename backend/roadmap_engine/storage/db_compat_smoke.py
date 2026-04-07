from __future__ import annotations

from backend.roadmap_engine.storage.database import get_connection, is_postgres_enabled


def run_db_compat_smoke() -> None:
    conn = get_connection()
    cursor = conn.cursor()

    table_name = "db_adapter_smoke_test"

    try:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.commit()

        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()

        cursor.execute(
            f"INSERT INTO {table_name} (name, is_active) VALUES (?, ?)",
            ("test-user", 1),
        )
        inserted_id = cursor.lastrowid
        if inserted_id is None:
            raise RuntimeError("INSERT succeeded but cursor.lastrowid is None")
        conn.commit()

        cursor.execute(
            f"SELECT id, name, is_active FROM {table_name} WHERE id = ?",
            (inserted_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("SELECT did not return inserted row")
        if int(row[0]) != int(inserted_id):
            raise RuntimeError("Selected id does not match inserted id")
        if str(row["name"]) != "test-user":
            raise RuntimeError("Selected row has unexpected name value")

        cursor.execute(
            f"UPDATE {table_name} SET name = ? WHERE id = ?",
            ("updated-user", inserted_id),
        )
        conn.commit()

        cursor.execute(
            f"SELECT name FROM {table_name} WHERE id = ?",
            (inserted_id,),
        )
        updated_row = cursor.fetchone()
        if updated_row is None or str(updated_row["name"]) != "updated-user":
            raise RuntimeError("UPDATE did not persist expected value")

        cursor.execute(
            f"DELETE FROM {table_name} WHERE id = ?",
            (inserted_id,),
        )
        conn.commit()

        cursor.execute(
            f"SELECT id FROM {table_name} WHERE id = ?",
            (inserted_id,),
        )
        deleted_row = cursor.fetchone()
        if deleted_row is not None:
            raise RuntimeError("DELETE did not remove test row")

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()

        backend_name = "PostgreSQL" if is_postgres_enabled() else "SQLite fallback"
        print(f"DB compatibility smoke test passed ({backend_name})")
        print(f"Inserted ID was: {inserted_id}")
    finally:
        conn.close()


if __name__ == "__main__":
    run_db_compat_smoke()
