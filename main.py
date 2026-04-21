import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from io import StringIO

from db import (
    connect_to_oracle,
    row_exists_messages,
    row_exists_tables_data,
)
from sql_builder import sql_for_messages, sql_for_tables_data
from file_loader import load_files_from_folder


# ---------------------------------------------------------------------------
# Pre-fetching helpers (bulk existence checks — avoids 1 query per row)
# ---------------------------------------------------------------------------

BATCH_SIZE = 500
CHUNK_SIZE = 1000  # Oracle IN-clause safe limit


# ---------------------------------------------------------------------------
# Pre-fetching helpers (scoped to keys in the current file only)
# ---------------------------------------------------------------------------

def _fetch_existing_message_keys(cursor, keys: list[str]) -> set:
    """
    Return the subset of `keys` that already exist in MLANG_MESSAGES.
    Queries in chunks to respect Oracle's 1000-item IN-clause limit.
    """
    existing = set()
    for i in range(0, len(keys), CHUNK_SIZE):
        chunk = keys[i:i + CHUNK_SIZE]
        placeholders = ", ".join(f":{j + 1}" for j in range(len(chunk)))
        cursor.execute(
            f"SELECT MESSAGE_KEY FROM BALADY.MLANG_MESSAGES WHERE MESSAGE_KEY IN ({placeholders})",
            chunk
        )
        existing.update(row[0] for row in cursor.fetchall())
    return existing


def _fetch_existing_tables_data_keys(cursor, table_name: str, keys: list[str]) -> set:
    """
    Return the subset of `keys` (row IDs) that already exist in MLANG_TABLES_DATA
    for the given table_name. Queries in chunks of CHUNK_SIZE.
    """
    existing = set()
    for i in range(0, len(keys), CHUNK_SIZE):
        chunk = keys[i:i + CHUNK_SIZE]
        placeholders = ", ".join(f":{j + 2}" for j in range(len(chunk)))
        cursor.execute(
            f"SELECT ID FROM BALADY.MLANG_TABLES_DATA "
            f"WHERE TABLE_NAME = :1 AND ID IN ({placeholders})",
            [table_name] + chunk
        )
        existing.update(row[0] for row in cursor.fetchall())
    return existing


# ---------------------------------------------------------------------------
# Batch DB write helpers
# ---------------------------------------------------------------------------


def _flush_batch(cursor, query, batch):
    """Execute a batch with executemany() and clear the list."""
    if batch:
        cursor.executemany(query, batch)
        batch.clear()


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

MERGE_TABLES_DATA = """
    MERGE INTO BALADY.MLANG_TABLES_DATA target
    USING (SELECT :1 AS TABLE_NAME, :2 AS ID, :3 AS COLUMN_NAME, :4 AS L2 FROM dual) source
    ON (target.TABLE_NAME = source.TABLE_NAME
        AND target.ID = source.ID
        AND target.COLUMN_NAME = source.COLUMN_NAME)
    WHEN MATCHED THEN
        UPDATE SET target.L2 = source.L2
    WHEN NOT MATCHED THEN
        INSERT (TABLE_NAME, ID, COLUMN_NAME, L2)
        VALUES (source.TABLE_NAME, source.ID, source.COLUMN_NAME, source.L2)
"""

MERGE_MESSAGES = """
    MERGE INTO BALADY.MLANG_MESSAGES target
    USING (SELECT :1 AS APP_NAME, :2 AS MESSAGE_KEY, :3 AS DEF_VALUE, :4 AS L2 FROM dual) source
    ON (target.MESSAGE_KEY = source.MESSAGE_KEY)
    WHEN MATCHED THEN
        UPDATE SET target.L2 = source.L2
    WHEN NOT MATCHED THEN
        INSERT (APP_NAME, MESSAGE_KEY, DEF_VALUE, L2)
        VALUES (source.APP_NAME, source.MESSAGE_KEY, source.DEF_VALUE, source.L2)
"""


def make_changes(cursor, folder_path, file_map, script_file):
    for table_name, file in tqdm(file_map.items(), desc="Tables", unit="table"):
        file_path = os.path.join(folder_path, file)
        df = pd.read_excel(file_path, dtype=str)
        df.fillna('', inplace=True)

        column_names = df.columns
        inserted = 0
        updated = 0
        batch = []
        sql_buffer = StringIO()

        sql_buffer.write(f"-- ============================================================\n")
        sql_buffer.write(f"-- Table: {table_name}\n")
        sql_buffer.write(f"-- ============================================================\n")

        if table_name == 'MESSAGES':
            # Only fetch keys that are present in this file — not the whole table
            all_keys = df['MESSAGE_KEY'].dropna().unique().tolist()
            existing_keys = _fetch_existing_message_keys(cursor, all_keys)

            for row in df.itertuples(index=False):
                translation_text = str(getattr(row, df.columns[-1])).strip()
                if not translation_text:
                    continue

                message_key = str(row.MESSAGE_KEY)
                default_value = str(row.DEF_VALUE)
                safe_text = translation_text.replace('&', 'and')
                exists = message_key in existing_keys

                batch.append(('BALADY', message_key, default_value, safe_text))
                sql_buffer.write(sql_for_messages('BALADY', message_key, default_value, safe_text, exists) + "\n")

                if exists:
                    updated += 1
                else:
                    inserted += 1
                    existing_keys.add(message_key)

                if len(batch) >= BATCH_SIZE:
                    _flush_batch(cursor, MERGE_MESSAGES, batch)

            _flush_batch(cursor, MERGE_MESSAGES, batch)

        else:
            cl_name = column_names[1]
            # Only fetch IDs that are present in this file — not the whole table
            all_ids = df.iloc[:, 0].dropna().unique().tolist()
            existing_keys = _fetch_existing_tables_data_keys(cursor, table_name, all_ids)

            for row in df.itertuples(index=False):
                translation_text = str(row[df.shape[1] - 1]).strip()
                if not translation_text:
                    continue

                row_id = str(row[0])
                safe_text = translation_text.replace('&', 'and')
                exists = row_id in existing_keys

                batch.append((table_name, row_id, cl_name, safe_text))
                sql_buffer.write(sql_for_tables_data(table_name, row_id, cl_name, safe_text, exists) + "\n")

                if exists:
                    updated += 1
                else:
                    inserted += 1
                    existing_keys.add(row_id)

                if len(batch) >= BATCH_SIZE:
                    _flush_batch(cursor, MERGE_TABLES_DATA, batch)

            _flush_batch(cursor, MERGE_TABLES_DATA, batch)

        summary = f"-- {table_name}: {inserted} inserted, {updated} updated\n\n"
        sql_buffer.write(summary)
        script_file.write(sql_buffer.getvalue())
        sql_buffer.close()
        tqdm.write(f"  {table_name}: {inserted} inserted, {updated} updated")


if __name__ == '__main__':
    folder_path = input("Enter the folder path containing the translation files: ").strip()
    while not os.path.exists(folder_path):
        print("Invalid path. Please try again.")
        folder_path = input("Enter the folder path containing the translation files: ").strip()

    file_map = load_files_from_folder(folder_path)
    connection = connect_to_oracle()

    script_path = os.path.join(folder_path, "ekram-dga-script.sql")

    try:
        cursor = connection.cursor()
        with open(script_path, "w", encoding="utf-8") as script_file:
            script_file.write("-- Translation script\n")
            script_file.write(f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            script_file.write("-- Generated By: Kareem Sayed\n\n")
            make_changes(cursor, folder_path, file_map, script_file)
            script_file.write("COMMIT;\n")
        connection.commit()
        print("Done. All changes committed.")
        print(f"SQL script saved to: {script_path}")
    except Exception as e:
        connection.rollback()
        print(f"Error: {e}. Changes rolled back.")
        raise
    finally:
        connection.close()
