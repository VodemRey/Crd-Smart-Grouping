"""Configuration settings for Smart Grouping pipeline."""


class Config:
    """Global configuration constants."""

    # File detection
    KEYS_FILENAME_TOKEN = "key"
    TABULAR_EXTENSIONS = [".xlsx", ".xls", ".csv"]

    # Keys (issuer reference) file requirements
    KEYS_REQUIRED_COLUMNS = [
        "issuer_name",
        "key",
    ]

    # Entry (items) file requirements
    ENTRY_FILE_REQUIRED_COLUMNS = [
        "amount",
        "reference_data",
        "currency",
    ]

    ENTRY_FILE_OPTIONAL_COLUMNS = [
        "item_type",
        "fund_name",
        "reconciliation_group",
    ]