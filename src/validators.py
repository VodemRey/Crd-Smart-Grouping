"""Shared validation helpers for the Smart Grouping pipeline."""

from config import Config


def validate_exactly_one_file(file_paths, file_label):
	"""Ensure exactly one file is discovered for a given input type."""
	file_count = len(file_paths)
	if file_count < 1:
		raise ValueError(f"{file_label} is not found")
	if file_count > 1:
		raise ValueError(f"More than 1 {file_label} were found")


def validate_required_columns(dataframe, required_columns, dataframe_label):
	"""Ensure all required columns exist in a dataframe."""
	missing_columns = sorted(set(required_columns).difference(dataframe.columns))
	if missing_columns:
		missing_columns_str = ", ".join(missing_columns)
		raise ValueError(
			f"{dataframe_label} is missing required columns: {missing_columns_str}"
		)


def validate_keys_required_columns(keys_df):
	"""Validate the required columns for the keys reference dataframe."""
	validate_required_columns(keys_df, Config.KEYS_REQUIRED_COLUMNS, "Keys data")


def validate_profile_name(profile_name, profiles):
	"""Validate that an explicitly provided profile exists."""
	if profile_name not in profiles:
		raise ValueError(f"Unknown profile: {profile_name}")


def validate_profile_detected(profile_name):
	"""Validate that profile detection produced a profile key."""
	if profile_name is None:
		raise ValueError("Entry profile is undefined")


def validate_unique_canonical_targets(target_columns):
	"""Validate that canonical target columns are unique."""
	targets = list(target_columns)
	duplicate_targets = {target for target in targets if targets.count(target) > 1}
	if duplicate_targets:
		duplicates_str = ", ".join(sorted(duplicate_targets))
		raise ValueError(
			f"Profile mapping contains non-unique canonical targets: {duplicates_str}"
		)


def validate_grouping_rules(grouping_rules):
	"""Validate that grouping rules include mandatory column role mappings."""
	mandatory_rule_keys = ["amount_column", "issuer_column", "segment_columns"]
	missing_rule_keys = [
		key for key in mandatory_rule_keys if key not in grouping_rules
	]
	if missing_rule_keys:
		missing_keys_str = ", ".join(missing_rule_keys)
		raise ValueError(
			f"Grouping rules are missing required settings: {missing_keys_str}"
		)

	if not isinstance(grouping_rules["segment_columns"], list):
		raise ValueError("Grouping rule 'segment_columns' must be a list")


def validate_grouping_input_columns(dataframe, grouping_rules):
	"""Validate that dataframe has all required columns from grouping rules."""
	validate_grouping_rules(grouping_rules)

	amount_column = grouping_rules["amount_column"]
	issuer_column = grouping_rules["issuer_column"]
	segment_columns = grouping_rules["segment_columns"]

	required_columns = [amount_column, issuer_column] + segment_columns
	validate_required_columns(
		dataframe,
		required_columns,
		"Grouping input data",
	)
