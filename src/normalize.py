"""Normalize input data into canonical grouping columns."""

from profiles import PROFILES


def data_normalize(entry_df, profile_name):
    """Add canonical gr_ columns and keep original source columns unchanged."""
    if profile_name not in PROFILES:
        raise ValueError(f"Unknown profile: {profile_name}")

    normalized_df = entry_df.copy()
    column_mapping = PROFILES[profile_name]["column_mapping"]

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

    targets = list(add_mapping.values())
    duplicate_targets = {target for target in targets if targets.count(target) > 1}
    if duplicate_targets:
        duplicates_str = ", ".join(sorted(duplicate_targets))
        raise ValueError(
            f"Profile mapping contains non-unique canonical targets: {duplicates_str}"
        )

    added_columns = []
    for source_col, target_col in add_mapping.items():
        normalized_df[target_col] = entry_df[source_col]
        if target_col not in added_columns:
            added_columns.append(target_col)

    if reference_source_columns:
        reference_series = (
            entry_df[reference_source_columns]
            .astype("string")
            .apply(lambda col: col.str.strip())
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

    return normalized_df