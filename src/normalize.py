"""Normalize input data into canonical grouping columns."""

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd

from profiles import PROFILES
from validators import (
    validate_keys_required_columns,
    validate_profile_name,
    validate_unique_canonical_targets,
)


def _apply_profile_normalization_rules(normalized_df, profile_rules):
    """Apply profile-specific normalization rules to canonical columns."""
    amount_sign_rule = profile_rules.get("amount_sign_from_item_type", {})
    if not amount_sign_rule.get("enabled"):
        return normalized_df

    item_type_column = amount_sign_rule.get("item_type_column", "gr_item_type")
    amount_column = amount_sign_rule.get("amount_column", "gr_amount")
    if item_type_column not in normalized_df.columns or amount_column not in normalized_df.columns:
        return normalized_df

    positive_prefixes = {
        prefix.lower() for prefix in amount_sign_rule.get("positive_prefixes", [])
    }
    negative_prefixes = {
        prefix.lower() for prefix in amount_sign_rule.get("negative_prefixes", [])
    }

    normalized_df[item_type_column] = (
        normalized_df[item_type_column]
        .astype("string")
        .fillna("")
        .str.lower()
        .str.replace(r"\s+", "", regex=True)
    )

    amount_numeric = pd.to_numeric(normalized_df[amount_column], errors="coerce")
    item_type_prefix = normalized_df[item_type_column].str[:3]

    positive_mask = item_type_prefix.isin(positive_prefixes)
    negative_mask = item_type_prefix.isin(negative_prefixes)

    normalized_df.loc[positive_mask, amount_column] = amount_numeric.loc[positive_mask].abs()
    normalized_df.loc[negative_mask, amount_column] = -amount_numeric.loc[negative_mask].abs()

    return normalized_df


def _parse_amount_to_cents(value):
    """Convert one raw amount value to integer cents using decimal-safe parsing."""
    if pd.isna(value):
        return pd.NA

    if isinstance(value, str):
        text_value = value.strip()
        if text_value == "":
            return pd.NA
    else:
        text_value = str(value).strip()

    is_parentheses_negative = text_value.startswith("(") and text_value.endswith(")")
    if is_parentheses_negative:
        text_value = text_value[1:-1]

    text_value = text_value.replace(",", "")
    text_value = text_value.replace("$", "")
    text_value = text_value.replace(" ", "")

    if text_value in {"", "+", "-", "."}:
        return pd.NA

    try:
        decimal_value = Decimal(text_value)
    except InvalidOperation:
        return pd.NA

    if is_parentheses_negative:
        decimal_value = -decimal_value

    cents_value = (decimal_value * Decimal("100")).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    cents_int = int(cents_value)

    int64_min = -(2**63)
    int64_max = 2**63 - 1
    if cents_int < int64_min or cents_int > int64_max:
        raise ValueError("Amount value exceeds supported Int64 cents range")

    return cents_int


def _add_amount_cents_column(normalized_df, amount_column="gr_amount", cents_column="gr_amount_cents"):
    """Add universal integer-cents amount column for grouping calculations."""
    if amount_column not in normalized_df.columns:
        return normalized_df

    amount_cents_series = normalized_df[amount_column].apply(_parse_amount_to_cents)
    normalized_df[cents_column] = pd.Series(
        amount_cents_series,
        index=normalized_df.index,
        dtype="Int64",
    )
    return normalized_df


def data_normalize(entry_df, keys_df, profile_name):
    """Normalize entry data and keys reference data for downstream matching."""
    validate_profile_name(profile_name, PROFILES)

    profile_data = PROFILES[profile_name]
    normalized_df = entry_df.copy()
    normalized_keys_df = keys_df.copy()
    column_mapping = profile_data["column_mapping"]

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

    profile_rules = profile_data.get("normalization_rules", {})
    normalized_df = _apply_profile_normalization_rules(normalized_df, profile_rules)
    normalized_df = _add_amount_cents_column(normalized_df)

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