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
