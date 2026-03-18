import pandas as pd
from profiles import PROFILES

def detect_profile(input_df):

    profile_name =  ""

    df_columns = set(input_df.columns)

    for profile_key, profile_data in PROFILES.items():
        required_columns = set(profile_data["column_mapping"].keys())
        if required_columns.issubset(df_columns):
            profile_name = profile_key
            break

    if not profile_name:
        raise ValueError("Entry profile is undefined")

    return profile_name
