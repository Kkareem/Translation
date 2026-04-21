import oracledb


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def connect_to_oracle():
    host = "RUHMPP-EXA-SCAN.MOMRA.NET"
    port = 1521
    service_name = "HIGH_QSMDEV.MOMRA.NET"
    username = "BALADY"
    password = "balady"

    dsn = f"{host}:{port}/{service_name}"
    return oracledb.connect(user=username, password=password, dsn=dsn)


# ---------------------------------------------------------------------------
# DB operations (MERGE-based upserts)
# ---------------------------------------------------------------------------

def insert_into_mlang_tables_data(cursor, table_name, row_id, cl_name, translation_text):
    query = """
            MERGE INTO BALADY.MLANG_TABLES_DATA target
            USING (SELECT :1 AS TABLE_NAME, :2 AS ID, :3 AS COLUMN_NAME, :4 AS L2 FROM dual) source
            ON (target.TABLE_NAME = source.TABLE_NAME AND target.ID = source.ID AND target.COLUMN_NAME = source.COLUMN_NAME)
            WHEN MATCHED THEN
                UPDATE SET target.L2 = source.L2
            WHEN NOT MATCHED THEN
                INSERT (TABLE_NAME, ID, COLUMN_NAME, L2)
                VALUES (source.TABLE_NAME, source.ID, source.COLUMN_NAME, source.L2)
            """
    cursor.execute(query, [table_name, row_id, cl_name, translation_text])


def insert_into_mlang_messages(cursor, app_name, message_key, default_value, translation_text):
    query = """
            MERGE INTO BALADY.MLANG_MESSAGES target
            USING (SELECT :1 AS APP_NAME, :2 AS MESSAGE_KEY, :3 AS DEF_VALUE, :4 AS L2 FROM dual) source
            ON (target.MESSAGE_KEY = source.MESSAGE_KEY)
            WHEN MATCHED THEN
                UPDATE SET target.L2 = source.L2
            WHEN NOT MATCHED THEN
                INSERT (APP_NAME, MESSAGE_KEY, DEF_VALUE, L2)
                VALUES (source.APP_NAME, source.MESSAGE_KEY, source.DEF_VALUE, source.L2)
            """
    cursor.execute(query, [app_name, message_key, default_value, translation_text])


def row_exists_messages(cursor, message_key):
    cursor.execute(
        "SELECT COUNT(*) FROM BALADY.MLANG_MESSAGES WHERE MESSAGE_KEY=:1",
        [message_key]
    )
    return cursor.fetchone()[0] > 0


def row_exists_tables_data(cursor, table_name, row_id, cl_name):
    cursor.execute(
        "SELECT COUNT(*) FROM BALADY.MLANG_TABLES_DATA WHERE TABLE_NAME=:1 AND ID=:2 AND COLUMN_NAME=:3",
        [table_name, row_id, cl_name]
    )
    return cursor.fetchone()[0] > 0