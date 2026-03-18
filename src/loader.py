"""Load and validate input datasets from project data folders."""

import pandas as pd
from validators import validate_exactly_one_file


def get_files(base_path):
    """Find and validate input and key value files in the data directories."""
    input_folder_path = base_path / "data" / "input"
    key_values_path = base_path / "data" / "static"
    input_file = []
    key_value_file = []
    tabular_extensions = {".xlsx", ".xls", ".csv"}

    for f in input_folder_path.iterdir():
        if f.suffix.lower() in tabular_extensions and f.is_file():
            input_file.append(f)

    validate_exactly_one_file(input_file, "Input file")

    for f in key_values_path.iterdir():
        if (
            f.suffix.lower() in tabular_extensions
            and f.is_file()
            and "key" in f.name.lower()
        ):
            key_value_file.append(f)

    validate_exactly_one_file(key_value_file, "Key values file")

    return input_file[0], key_value_file[0]


def read_files(input_file_path, key_values_file_path):
    """Read input and key value files into pandas DataFrames."""
    if input_file_path.suffix.lower() == ".csv":
        input_df = pd.read_csv(input_file_path)
    else:
        input_df = pd.read_excel(input_file_path)

    if key_values_file_path.suffix.lower() == ".csv":
        key_values_df = pd.read_csv(key_values_file_path)
    else:
        key_values_df = pd.read_excel(key_values_file_path)

    return input_df, key_values_df