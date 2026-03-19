"""Grouping step scaffolding for profile-driven configuration."""

import pandas as pd

from profiles import PROFILES
from validators import (
	validate_grouping_input_columns,
	validate_grouping_rules,
	validate_profile_name,
)


GROUP_NUMBER_COLUMN = "group_number"
GROUPING_STAGE_COLUMN = "grouping_stage"


def _run_noop_pass(grouped_input_df, segment_indexes, context, pass_config, next_group_number):
	"""Default placeholder pass implementation for future grouping algorithms."""
	return {
		"dataframe": grouped_input_df,
		"next_group_number": next_group_number,
		"groups_found": 0,
		"rows_used": 0,
		"pass_name": pass_config.get("name", "unknown_pass"),
		"segment_size": len(segment_indexes),
	}


PASS_REGISTRY = {
	"pair_exact": _run_noop_pass,
	"triple_balanced": _run_noop_pass,
	"combo_4_5": _run_noop_pass,
}


def get_grouping_context(grouped_input_df, profile_name):
	"""Resolve and validate grouping configuration for a profile."""
	validate_profile_name(profile_name, PROFILES)

	profile_data = PROFILES[profile_name]
	grouping_rules = profile_data.get("grouping_rules", {})
	validate_grouping_rules(grouping_rules)
	validate_grouping_input_columns(grouped_input_df, grouping_rules)

	return {
		"amount_column": grouping_rules["amount_column"],
		"amount_cents_column": grouping_rules["amount_cents_column"],
		"issuer_column": grouping_rules["issuer_column"],
		"segment_columns": grouping_rules["segment_columns"],
		"segment_stages": grouping_rules["segment_stages"],
		"pass_pipeline": grouping_rules["pass_pipeline"],
		"tolerance_cents": grouping_rules.get("tolerance_cents", 2),
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
	prepared_df = ensure_group_number_column(grouped_input_df)
	if GROUPING_STAGE_COLUMN not in prepared_df.columns:
		prepared_df = prepared_df.copy()
		prepared_df[GROUPING_STAGE_COLUMN] = pd.Series(
			pd.NA,
			index=prepared_df.index,
			dtype="string",
		)
	return prepared_df


def _get_unassigned_mask(grouped_input_df, group_column=GROUP_NUMBER_COLUMN):
	"""Return boolean mask for rows that are not grouped yet."""
	return grouped_input_df[group_column].isna()


def _build_stage_mask(grouped_input_df, context, stage_config):
	"""Build row mask for one stage based on issuer requirements."""
	issuer_column = context["issuer_column"]
	require_issuer = stage_config.get("require_issuer", False)

	issuer_present_mask = (
		grouped_input_df[issuer_column]
		.astype("string")
		.fillna("")
		.str.strip()
		.ne("")
	)
	if require_issuer:
		return issuer_present_mask
	return ~issuer_present_mask


def _iter_segment_indexes(grouped_input_df, context, stage_config):
	"""Yield row index groups for one stage segmented by configured keys."""
	segment_columns = context["segment_columns"]
	unassigned_mask = _get_unassigned_mask(grouped_input_df)
	stage_mask = _build_stage_mask(grouped_input_df, context, stage_config)
	base_mask = unassigned_mask & stage_mask

	eligible_df = grouped_input_df.loc[base_mask]
	if eligible_df.empty:
		return

	for _, segment_df in eligible_df.groupby(segment_columns, dropna=False, sort=True):
		yield list(segment_df.index)


def run_grouping_pipeline(grouped_input_df, profile_name):
	"""Run configurable grouping stages and passes in profile-defined order."""
	context = get_grouping_context(grouped_input_df, profile_name)
	working_df = prepare_grouping_dataframe(grouped_input_df, profile_name)
	next_group_number = get_next_group_number(working_df)
	pipeline_stats = []

	for stage_config in context["segment_stages"]:
		stage_name = stage_config.get("name", "unnamed_stage")
		for segment_indexes in _iter_segment_indexes(working_df, context, stage_config):
			for pass_config in context["pass_pipeline"]:
				if not pass_config.get("enabled", True):
					continue

				pass_name = pass_config["name"]
				pass_runner = PASS_REGISTRY.get(pass_name)
				if pass_runner is None:
					raise ValueError(
						f"Grouping pass '{pass_name}' is not registered in PASS_REGISTRY"
					)

				pass_result = pass_runner(
					working_df,
					segment_indexes,
					context,
					pass_config,
					next_group_number,
				)
				working_df = pass_result["dataframe"]
				next_group_number = pass_result["next_group_number"]
				pipeline_stats.append(
					{
						"stage_name": stage_name,
						"pass_name": pass_result["pass_name"],
						"segment_size": pass_result["segment_size"],
						"groups_found": pass_result["groups_found"],
						"rows_used": pass_result["rows_used"],
					}
				)

	if pipeline_stats:
		working_df.attrs["grouping_stats"] = pipeline_stats

	return working_df
