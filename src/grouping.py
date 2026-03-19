"""Grouping step scaffolding for profile-driven configuration."""

import pandas as pd

from profiles import PROFILES
from validators import (
	validate_grouping_input_columns,
	validate_grouping_rules,
	validate_profile_name,
)


GROUP_NUMBER_COLUMN = "group_number"


def get_grouping_context(grouped_input_df, profile_name):
	"""Resolve and validate grouping configuration for a profile."""
	validate_profile_name(profile_name, PROFILES)

	profile_data = PROFILES[profile_name]
	grouping_rules = profile_data.get("grouping_rules", {})
	validate_grouping_rules(grouping_rules)
	validate_grouping_input_columns(grouped_input_df, grouping_rules)

	return {
		"amount_column": grouping_rules["amount_column"],
		"issuer_column": grouping_rules["issuer_column"],
		"segment_columns": grouping_rules["segment_columns"],
		"grouping_rules": grouping_rules,
	}


def ensure_group_number_column(grouped_input_df, column_name=GROUP_NUMBER_COLUMN):
	"""Ensure that grouping output has a nullable integer group number column."""
	if column_name in grouped_input_df.columns:
		return grouped_input_df

	prepared_df = grouped_input_df.copy()
	prepared_df[column_name] = pd.Series(pd.NA, index=prepared_df.index, dtype="Int64")
	return prepared_df


def get_next_group_number(grouped_input_df, column_name=GROUP_NUMBER_COLUMN):
	"""Return the next available group number based on existing assignments."""
	if column_name not in grouped_input_df.columns:
		return 1

	assigned_group_numbers = grouped_input_df[column_name].dropna()
	if assigned_group_numbers.empty:
		return 1

	return int(assigned_group_numbers.max()) + 1


def assign_group_number(
	grouped_input_df,
	row_indexes,
	group_number,
	column_name=GROUP_NUMBER_COLUMN,
):
	"""Assign one group number to all rows that belong to a matched group."""
	prepared_df = ensure_group_number_column(grouped_input_df, column_name)
	prepared_df.loc[list(row_indexes), column_name] = int(group_number)
	return prepared_df


def prepare_grouping_dataframe(grouped_input_df, profile_name):
	"""Validate grouping contract and initialize group number column."""
	get_grouping_context(grouped_input_df, profile_name)
	return ensure_group_number_column(grouped_input_df)
