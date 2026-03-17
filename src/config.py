class Config:
    
    KEYS_FILENAME_TOKEN = "key"

    TABULAR_EXTENSIONS = [
        ".xlsx",
        ".xls",
        ".csv"
    ]

    KEYS_REQUIRED_COLUMNS = [
        "issuer_name",
        "key"
    ]

    ENTRY_FILE_REQUIRED_COLUMNS = [
        "Amount",
        "Reference data",
        "Currency"
    ]

    ENTRY_FILE_OPTIONAL_COLUMNS = [
        "Item Type",
        "Fund name",
        "Reconciliation group"
    ]