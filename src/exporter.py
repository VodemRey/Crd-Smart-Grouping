from pathlib import Path

import pandas as pd

# Columns that should always appear first in the output, in this order.
_LEADING_COLUMNS = [
    "group_number",
    "grouping_stage",
    "gr_assigned_issuer",
    "gr_keys_found",
]


def reorder_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return df with output-priority columns moved to the front.

    Order:
      1. Columns listed in _LEADING_COLUMNS (only those that exist)
      2. Remaining gr_* columns (preserving their current relative order)
      3. All other original columns (preserving their current relative order)
    """
    existing = list(df.columns)

    leading = [c for c in _LEADING_COLUMNS if c in existing]
    after_leading = [c for c in existing if c not in leading]
    gr_rest = [c for c in after_leading if c.startswith("gr_")]
    original = [c for c in after_leading if not c.startswith("gr_")]

    return df[leading + gr_rest + original]


def sort_output(df: pd.DataFrame) -> pd.DataFrame:
  """Sort rows by group_number ascending; ungrouped rows go to the bottom."""
  group_col = "group_number"
  if group_col not in df.columns:
    return df
  grouped_mask = df[group_col].notna()
  grouped_part = df[grouped_mask].sort_values(group_col, ascending=True)
  ungrouped_part = df[~grouped_mask]
  return pd.concat([grouped_part, ungrouped_part], ignore_index=True)


def save_output(df: pd.DataFrame, input_file_path: Path, base_path: Path) -> Path:
  """Save final output dataframe into data/output as <entry_file>_grouped.<ext>."""
  output_dir = base_path / "data" / "output"
  output_dir.mkdir(parents=True, exist_ok=True)

  suffix = input_file_path.suffix.lower()
  if suffix == ".csv":
    output_path = output_dir / f"{input_file_path.stem}_grouped.csv"
    df.to_csv(output_path, index=False)
    return output_path

  output_path = output_dir / f"{input_file_path.stem}_grouped.xlsx"
  df.to_excel(output_path, index=False)
  return output_path
