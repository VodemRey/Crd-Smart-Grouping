"""Grouping engine: profile-driven stage/pass orchestration and zero-sum detection."""

import pandas as pd

from profiles import PROFILES
from validators import (
	validate_grouping_input_columns,
	validate_grouping_rules,
	validate_profile_name,
)


GROUP_NUMBER_COLUMN = "group_number"
GROUPING_STAGE_COLUMN = "grouping_stage"

# Safety ceiling for the O(n^3) 5-row search.  Segments larger than this
# are skipped in the 5-row sub-pass of combo_4_5 to prevent stalls.
_COMBO_5_MAX_SEGMENT = 200

# Safety ceiling for the O(n^2) 4-row pair-of-pairs search.
_COMBO_4_MAX_SEGMENT = 500


def _make_pass_result(pass_config, df, next_group_number, groups_found, rows_used, segment_size):
	"""Build the standard pass result dict expected by the orchestrator."""
	return {
		"dataframe": df,
		"next_group_number": next_group_number,
		"groups_found": groups_found,
		"rows_used": rows_used,
		"pass_name": pass_config.get("name", "unknown_pass"),
		"segment_size": segment_size,
	}


def _get_unassigned_in_segment(df, segment_indexes):
	"""Return the subset of segment_indexes whose group_number is still NA."""
	return [idx for idx in segment_indexes if pd.isna(df.at[idx, GROUP_NUMBER_COLUMN])]


def _check_group_constraints(df, candidate_indexes, context):
	"""Return True if the candidate group satisfies all profile constraints.

	Constraint - issuer compatibility:
	    At most one non-empty issuer value can appear in a group.
	    Allowed: issuer-only, issuer+blank, blank+blank.
	    Forbidden: two or more different non-empty issuers.

	Constraint - require_at_least_one_l_in_group:
	    The group must contain at least one row whose gr_item_type starts with
	    the configured l_prefix, meaning not all rows are S-type entries.
	"""
	issuer_col = context.get("issuer_column", "gr_assigned_issuer")
	if issuer_col in df.columns:
		non_empty_issuers = {
			str(df.at[idx, issuer_col]).strip()
			for idx in candidate_indexes
			if pd.notna(df.at[idx, issuer_col]) and str(df.at[idx, issuer_col]).strip()
		}
		if len(non_empty_issuers) > 1:
			return False

	rule = context["grouping_rules"].get("require_at_least_one_l_in_group", {})
	if not rule.get("enabled", False):
		return True
	col = rule.get("item_type_column", "gr_item_type")
	prefix = rule.get("l_prefix", "l").lower()
	if col not in df.columns:
		return True
	return any(
		pd.notna(df.at[idx, col]) and str(df.at[idx, col]).lower().startswith(prefix)
		for idx in candidate_indexes
	)


def _build_cents_map(df, indexes, cents_col):
	"""Build {int_cents_value: [idx, ...]} for the given index list."""
	result = {}
	for idx in indexes:
		v = df.at[idx, cents_col]
		if pd.isna(v):
			continue
		result.setdefault(int(v), []).append(idx)
	return result


def _run_noop_pass(df, segment_indexes, context, pass_config, next_group_number, stage_name):
	"""Placeholder pass - registers in PASS_REGISTRY without doing anything."""
	return _make_pass_result(pass_config, df, next_group_number, 0, 0, len(segment_indexes))


def _run_pair_exact(df, segment_indexes, context, pass_config, next_group_number, stage_name):
	"""Find pairs whose gr_amount_cents sum is within tolerance_cents of zero.

	Algorithm: greedy hash-map scan - O(n * tol).
	"""
	tol = context["tolerance_cents"]
	cents_col = context["amount_cents_column"]

	unassigned = [
		idx for idx in _get_unassigned_in_segment(df, segment_indexes)
		if not pd.isna(df.at[idx, cents_col])
	]
	if len(unassigned) < 2:
		return _make_pass_result(pass_config, df, next_group_number, 0, 0, len(segment_indexes))

	cents_map = _build_cents_map(df, unassigned, cents_col)
	used = set()
	groups_found = 0
	rows_used = 0

	for idx in unassigned:
		if idx in used:
			continue
		v = int(df.at[idx, cents_col])

		partner = None
		for target in range(-v - tol, -v + tol + 1):
			candidates = [i for i in cents_map.get(target, []) if i != idx and i not in used]
			if candidates:
				partner = candidates[0]
				break

		if partner is None:
			continue

		group_rows = [idx, partner]
		if not _check_group_constraints(df, group_rows, context):
			continue

		df = assign_group_number(df, group_rows, next_group_number)
		df.loc[group_rows, GROUPING_STAGE_COLUMN] = stage_name
		used.update(group_rows)
		next_group_number += 1
		groups_found += 1
		rows_used += 2

	return _make_pass_result(pass_config, df, next_group_number, groups_found, rows_used, len(segment_indexes))


def _run_triple_balanced(df, segment_indexes, context, pass_config, next_group_number, stage_name):
	"""Find 3-row groups whose gr_amount_cents sum is within tolerance_cents of zero.

	Algorithm: greedy nested scan with hash-map lookup - O(n^2 * tol).
	"""
	tol = context["tolerance_cents"]
	cents_col = context["amount_cents_column"]

	unassigned = [
		idx for idx in _get_unassigned_in_segment(df, segment_indexes)
		if not pd.isna(df.at[idx, cents_col])
	]
	if len(unassigned) < 3:
		return _make_pass_result(pass_config, df, next_group_number, 0, 0, len(segment_indexes))

	cents_map = _build_cents_map(df, unassigned, cents_col)
	used = set()
	groups_found = 0
	rows_used = 0

	for i, idx_a in enumerate(unassigned):
		if idx_a in used:
			continue
		v_a = int(df.at[idx_a, cents_col])
		found_for_a = False

		for idx_b in unassigned[i + 1:]:
			if found_for_a:
				break
			if idx_b in used:
				continue
			v_b = int(df.at[idx_b, cents_col])
			target = -(v_a + v_b)

			partner = None
			for t in range(target - tol, target + tol + 1):
				candidates = [
					k for k in cents_map.get(t, [])
					if k not in (idx_a, idx_b) and k not in used
				]
				if candidates:
					partner = candidates[0]
					break

			if partner is None:
				continue

			group_rows = [idx_a, idx_b, partner]
			if not _check_group_constraints(df, group_rows, context):
				continue

			df = assign_group_number(df, group_rows, next_group_number)
			df.loc[group_rows, GROUPING_STAGE_COLUMN] = stage_name
			used.update(group_rows)
			next_group_number += 1
			groups_found += 1
			rows_used += 3
			found_for_a = True

	return _make_pass_result(pass_config, df, next_group_number, groups_found, rows_used, len(segment_indexes))


def _run_combo_4_5(df, segment_indexes, context, pass_config, next_group_number, stage_name):
	"""Find 4- and 5-row zero-sum groups using a meet-in-the-middle strategy.

	Finds all valid 4-row groups (pair-of-pairs complement search), then 5-row
	groups (triple + pair) from the remaining unassigned rows.
	The 5-row sub-pass is skipped when the segment exceeds _COMBO_5_MAX_SEGMENT
	to prevent O(n^3) stalls on large datasets.
	"""
	tol = context["tolerance_cents"]
	cents_col = context["amount_cents_column"]
	groups_found = 0
	rows_used = 0

	# 4-row sub-pass
	unassigned4 = [
		idx for idx in _get_unassigned_in_segment(df, segment_indexes)
		if not pd.isna(df.at[idx, cents_col])
	]
	if 4 <= len(unassigned4) <= _COMBO_4_MAX_SEGMENT:
		pair_sums4 = {}
		for i, idx_a in enumerate(unassigned4):
			v_a = int(df.at[idx_a, cents_col])
			for idx_b in unassigned4[i + 1:]:
				v_b = int(df.at[idx_b, cents_col])
				pair_sums4.setdefault(v_a + v_b, []).append((idx_a, idx_b))

		used4 = set()
		for i, idx_a in enumerate(unassigned4):
			if idx_a in used4:
				continue
			v_a = int(df.at[idx_a, cents_col])
			found_for_a = False

			for idx_b in unassigned4[i + 1:]:
				if found_for_a:
					break
				if idx_b in used4:
					continue
				v_b = int(df.at[idx_b, cents_col])
				target = -(v_a + v_b)

				group_rows = None
				for t in range(target - tol, target + tol + 1):
					for idx_c, idx_d in pair_sums4.get(t, []):
						if idx_c in used4 or idx_d in used4:
							continue
						if idx_c in (idx_a, idx_b) or idx_d in (idx_a, idx_b):
							continue
						group_rows = [idx_a, idx_b, idx_c, idx_d]
						break
					if group_rows:
						break

				if group_rows is None:
					continue
				if not _check_group_constraints(df, group_rows, context):
					continue

				df = assign_group_number(df, group_rows, next_group_number)
				df.loc[group_rows, GROUPING_STAGE_COLUMN] = stage_name
				used4.update(group_rows)
				next_group_number += 1
				groups_found += 1
				rows_used += 4
				found_for_a = True

	# 5-row sub-pass
	unassigned5 = [
		idx for idx in _get_unassigned_in_segment(df, segment_indexes)
		if not pd.isna(df.at[idx, cents_col])
	]
	if 5 <= len(unassigned5) <= _COMBO_5_MAX_SEGMENT:
		pair_sums5 = {}
		for i, idx_a in enumerate(unassigned5):
			v_a = int(df.at[idx_a, cents_col])
			for idx_b in unassigned5[i + 1:]:
				v_b = int(df.at[idx_b, cents_col])
				pair_sums5.setdefault(v_a + v_b, []).append((idx_a, idx_b))

		used5 = set()
		for i, idx_a in enumerate(unassigned5):
			if idx_a in used5:
				continue
			v_a = int(df.at[idx_a, cents_col])
			found_for_a = False

			for j in range(i + 1, len(unassigned5)):
				if found_for_a:
					break
				idx_b = unassigned5[j]
				if idx_b in used5:
					continue
				v_b = int(df.at[idx_b, cents_col])

				for k in range(j + 1, len(unassigned5)):
					idx_c = unassigned5[k]
					if idx_c in used5:
						continue
					v_c = int(df.at[idx_c, cents_col])
					sum_abc = v_a + v_b + v_c
					target = -sum_abc

					group_rows = None
					for t in range(target - tol, target + tol + 1):
						for idx_d, idx_e in pair_sums5.get(t, []):
							if idx_d in used5 or idx_e in used5:
								continue
							if any(x in (idx_a, idx_b, idx_c) for x in (idx_d, idx_e)):
								continue
							group_rows = [idx_a, idx_b, idx_c, idx_d, idx_e]
							break
						if group_rows:
							break

					if group_rows is None:
						continue
					if not _check_group_constraints(df, group_rows, context):
						continue

					df = assign_group_number(df, group_rows, next_group_number)
					df.loc[group_rows, GROUPING_STAGE_COLUMN] = stage_name
					used5.update(group_rows)
					next_group_number += 1
					groups_found += 1
					rows_used += 5
					found_for_a = True
					break  # break k loop

				if found_for_a:
					break  # break j loop

	return _make_pass_result(pass_config, df, next_group_number, groups_found, rows_used, len(segment_indexes))


PASS_REGISTRY = {
	"pair_exact": _run_pair_exact,
	"triple_balanced": _run_triple_balanced,
	"combo_4_5": _run_combo_4_5,
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


def _has_issuer_mask(grouped_input_df, issuer_column):
	"""Return boolean mask for rows where the issuer field is non-empty."""
	return (
		grouped_input_df[issuer_column]
		.astype("string")
		.fillna("")
		.str.strip()
		.ne("")
	)


def _build_stage_mask(grouped_input_df, context, stage_config):
	"""Legacy mask builder for stages using the require_issuer flag."""
	issuer_column = context["issuer_column"]
	if stage_config.get("require_issuer", False):
		return _has_issuer_mask(grouped_input_df, issuer_column)
	return pd.Series(True, index=grouped_input_df.index, dtype=bool)


def _iter_segment_indexes(grouped_input_df, context, stage_config):
	"""Yield row index lists for one stage.

	Mode "A"  — assigned (B) rows only, segmented by (issuer, currency, recon).
	Mode "BC" — for each issuer: its remaining B rows + all C rows that share
	            the same (currency, recon).  One segment per issuer x seg-key.
	Mode "CC" — unassigned (C) rows only, segmented by (currency, recon).
	Legacy    — falls back to old require_issuer behaviour when 'mode' absent.
	"""
	segment_columns = context["segment_columns"]
	issuer_column = context["issuer_column"]
	mode = stage_config.get("mode")
	unassigned_mask = _get_unassigned_mask(grouped_input_df)

	if mode == "A":
		b_mask = unassigned_mask & _has_issuer_mask(grouped_input_df, issuer_column)
		eligible_df = grouped_input_df.loc[b_mask]
		if eligible_df.empty:
			return
		group_cols = [issuer_column] + segment_columns
		for _, seg_df in eligible_df.groupby(group_cols, dropna=False, sort=True):
			yield list(seg_df.index)

	elif mode == "BC":
		him = _has_issuer_mask(grouped_input_df, issuer_column)
		b_df = grouped_input_df.loc[unassigned_mask & him]
		c_df = grouped_input_df.loc[unassigned_mask & ~him]
		if b_df.empty:
			return
		issuers = sorted(b_df[issuer_column].astype("string").dropna().unique())
		for issuer in issuers:
			issuer_str = str(issuer).strip()
			if not issuer_str:
				continue
			issuer_b = b_df[
				b_df[issuer_column].astype("string").str.strip() == issuer_str
			]
			if issuer_b.empty:
				continue
			for seg_keys, b_seg_df in issuer_b.groupby(
				segment_columns, dropna=False, sort=True
			):
				if not c_df.empty:
					keys_tuple = (seg_keys,) if len(segment_columns) == 1 else seg_keys
					c_mask = pd.Series(True, index=c_df.index)
					for col, key in zip(segment_columns, keys_tuple):
						if pd.isna(key):
							c_mask &= c_df[col].isna()
						else:
							c_mask &= c_df[col] == key
					c_seg_df = c_df.loc[c_mask]
				else:
					c_seg_df = c_df  # empty
				seg_idxs = list(b_seg_df.index) + list(c_seg_df.index)
				if len(seg_idxs) >= 2:
					yield seg_idxs

	elif mode == "CC":
		him = _has_issuer_mask(grouped_input_df, issuer_column)
		c_mask = unassigned_mask & ~him
		eligible_df = grouped_input_df.loc[c_mask]
		if eligible_df.empty:
			return
		for _, seg_df in eligible_df.groupby(segment_columns, dropna=False, sort=True):
			yield list(seg_df.index)

	else:
		# Legacy fallback: require_issuer flag
		stage_mask = _build_stage_mask(grouped_input_df, context, stage_config)
		base_mask = unassigned_mask & stage_mask
		eligible_df = grouped_input_df.loc[base_mask]
		if eligible_df.empty:
			return
		for _, seg_df in eligible_df.groupby(segment_columns, dropna=False, sort=True):
			yield list(seg_df.index)


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
					stage_name,
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
