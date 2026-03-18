"""Issuer assignment step for normalized reconciliation data.

Matches issuer keys against ``gr_reference_data`` and appends assignment columns.
"""

import pandas as pd
from validators import validate_keys_required_columns, validate_required_columns


def assign_issuers(normalized_df, keys_df):
    """Assign issuer and matched keys for each normalized entry row."""
    validate_required_columns(
        normalized_df, ["gr_reference_data"], "Normalized data"
    )
    validate_keys_required_columns(keys_df)

    issuers_assigned_df = normalized_df.copy()

    keys_sorted_df = keys_df.copy()
    keys_sorted_df["_key_len"] = (
        keys_sorted_df["key"].astype("string").str.len().fillna(0).astype(int)
    )
    keys_sorted_df = keys_sorted_df.sort_values(
        by="_key_len", ascending=False, kind="mergesort"
    )

    key_issuer_pairs = []
    for _, row in keys_sorted_df.iterrows():
        key_value = row["key"]
        issuer_value = row["issuer_name"]

        if pd.isna(key_value):
            continue

        key_text = str(key_value)
        if key_text == "":
            continue

        key_issuer_pairs.append((key_text, issuer_value))

    assigned_issuers = []
    keys_found_values = []

    for reference_value in issuers_assigned_df["gr_reference_data"]:
        if pd.isna(reference_value):
            reference_text = ""
        else:
            reference_text = str(reference_value)

        matched_pairs = []
        for key_text, issuer_value in key_issuer_pairs:
            if key_text in reference_text:
                matched_pairs.append((key_text, issuer_value))

        if not matched_pairs:
            assigned_issuers.append("")
            keys_found_values.append("not found")
            continue

        assigned_issuers.append(matched_pairs[0][1])

        ordered_unique_keys = []
        for key_text, _ in matched_pairs:
            if key_text not in ordered_unique_keys:
                ordered_unique_keys.append(key_text)
        keys_found_values.append(", ".join(ordered_unique_keys))

    issuers_assigned_df["gr_assigned_issuer"] = assigned_issuers
    issuers_assigned_df["gr_keys_found"] = keys_found_values

    front_columns = ["gr_assigned_issuer", "gr_keys_found"]
    remaining_columns = [
        col for col in issuers_assigned_df.columns if col not in front_columns
    ]
    issuers_assigned_df = issuers_assigned_df[front_columns + remaining_columns]

    return issuers_assigned_df