import os


def load_files_from_folder(folder_path):
    """Return a dict mapping table name -> Excel file name for the given folder."""
    files = {}
    for file in os.listdir(folder_path):
        if file.endswith(('.xlsx', '.xls')):
            table_name = file.split('.')[0]
            files[table_name] = file
    return files