"""Detect input dataset profile by matching required source columns."""

from profiles import PROFILES


def detect_profile(input_df):
    """Return profile key if input columns match a known profile."""
    df_columns = set(input_df.columns)

    for profile_key, profile_data in PROFILES.items():
        required_columns = set(profile_data["column_mapping"].keys())
        if required_columns.issubset(df_columns):
            return profile_key

    raise ValueError("Entry profile is undefined")
