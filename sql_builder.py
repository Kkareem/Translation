def _esc(value):
    """Escape single quotes for safe embedding in SQL literals."""
    return str(value).replace("'", "''")

def sql_for_tables_data(table_name, row_id, cl_name, translation_text, exists):
    if exists:
        return (
            f"UPDATE BALADY.MLANG_TABLES_DATA "
            f"SET L2 = '{_esc(translation_text)}' "
            f"WHERE TABLE_NAME = '{_esc(table_name)}' "
            f"AND ID = '{_esc(row_id)}' "
            f"AND COLUMN_NAME = '{_esc(cl_name)}';"
        )
    return (
        f"INSERT INTO BALADY.MLANG_TABLES_DATA (TABLE_NAME, ID, COLUMN_NAME, L2) "
        f"VALUES ('{_esc(table_name)}', '{_esc(row_id)}', '{_esc(cl_name)}', '{_esc(translation_text)}');"
    )


def sql_for_messages(app_name, message_key, default_value, translation_text, exists):
    if exists:
        return (
            f"UPDATE BALADY.MLANG_MESSAGES "
            f"SET L2 = '{_esc(translation_text)}' "
            f"WHERE MESSAGE_KEY = '{_esc(message_key)}';"
        )
    return (
        f"INSERT INTO BALADY.MLANG_MESSAGES (APP_NAME, MESSAGE_KEY, DEF_VALUE, L2) "
        f"VALUES ('{_esc(app_name)}', '{_esc(message_key)}', '{_esc(default_value)}', '{_esc(translation_text)}');"
    )