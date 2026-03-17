import pandas as pd
from pathlib import Path


def get_files(base_path):
    """Find and validate input and key value files in the data directories."""
    input_folder_path = base_path / "data" / "input"
    key_values_path = base_path / "data" / "static"
    input_file = []
    key_value_file = []

    for f in input_folder_path.iterdir():
        if f.suffix.lower() in [".xlsx", ".xls", ".csv"] and f.is_file():
            input_file.append(f)

    if len(input_file) < 1:
        raise ValueError("Input file is not found")
    elif len(input_file) > 1:
        raise ValueError("More than 1 input file were found")

    for f in key_values_path.iterdir():
        if (
            f.suffix.lower() in [".xlsx", ".xls", ".csv"]
            and f.is_file()
            and "key" in f.name.lower()
        ):
            key_value_file.append(f)

    if len(key_value_file) < 1:
        raise ValueError("Key values file is not found")
    elif len(key_value_file) > 1:
        raise ValueError("More than 1 key values file were found")

    return input_file[0], key_value_file[0]


def read_files(input_file, key_values_file):
    """Read input and key value files into pandas DataFrames."""
    if input_file.suffix.lower() == ".csv":
        input_df = pd.read_csv(input_file)
    else:
        input_df = pd.read_excel(input_file)

    if key_values_file.suffix.lower() == ".csv":
        key_values_df = pd.read_csv(key_values_file)
    else:
        key_values_df = pd.read_excel(key_values_file)

    return input_df, key_values_df