"""Normalize input data into canonical grouping columns."""

from profiles import PROFILES
from validators import (
    validate_keys_required_columns,
    validate_profile_name,
    validate_unique_canonical_targets,
)


def data_normalize(entry_df, keys_df, profile_name):
    """Normalize entry data and keys reference data for downstream matching."""
    validate_profile_name(profile_name, PROFILES)

    normalized_df = entry_df.copy()
    normalized_keys_df = keys_df.copy()
    column_mapping = PROFILES[profile_name]["column_mapping"]

    validate_keys_required_columns(normalized_keys_df)

    normalized_keys_df["key"] = (
        normalized_keys_df["key"]
        .astype("string")
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w\s]|_", "", regex=True)
    )

    reference_source_columns = [
        source_col
        for source_col, target_col in column_mapping.items()
        if target_col == "gr_reference_data" and source_col in normalized_df.columns
    ]

    add_mapping = {
        source_col: target_col
        for source_col, target_col in column_mapping.items()
        if source_col in normalized_df.columns and target_col != "gr_reference_data"
    }

    validate_unique_canonical_targets(add_mapping.values())

    added_columns = []
    for source_col, target_col in add_mapping.items():
        normalized_df[target_col] = entry_df[source_col]
        if target_col not in added_columns:
            added_columns.append(target_col)

    if reference_source_columns:
        reference_series = (
            entry_df[reference_source_columns]
            .astype("string")
            .apply(lambda col: col.str.strip().str.lower())
            .replace("", None)
            .apply(lambda row: " ".join(value for value in row.dropna()), axis=1)
        )
        normalized_df["gr_reference_data"] = reference_series
        added_columns.append("gr_reference_data")

    # Put newly added canonical columns at the beginning, preserving source columns.
    if added_columns:
        unique_added_columns = []
        for col in added_columns:
            if col not in unique_added_columns:
                unique_added_columns.append(col)

        original_columns = [
            col for col in normalized_df.columns if col not in unique_added_columns
        ]
        normalized_df = normalized_df[unique_added_columns + original_columns]

    return normalized_df, normalized_keys_df