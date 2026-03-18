"""Detect input dataset profile by matching required source columns."""

from profiles import PROFILES
from validators import validate_profile_detected


def detect_profile(input_df):
    """Return profile key if input columns match a known profile."""
    df_columns = set(input_df.columns)
    matched_profile = None

    for profile_key, profile_data in PROFILES.items():
        required_columns = set(profile_data["column_mapping"].keys())
        if required_columns.issubset(df_columns):
            matched_profile = profile_key
            break

    validate_profile_detected(matched_profile)
    return matched_profile
