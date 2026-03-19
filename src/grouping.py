"""Grouping step scaffolding for profile-driven configuration."""

from profiles import PROFILES
from validators import (
	validate_grouping_input_columns,
	validate_grouping_rules,
	validate_profile_name,
)


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
