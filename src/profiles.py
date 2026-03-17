"""Input profile mappings for supported datasets.

Profiles define how to interpret and normalize spreadsheet columns
for each supported dataset type. Use this module to configure
column mappings and reference field definitions.
"""


ITEMS_STATUS_PROFILE = {
    "name": "Items Status",
    "column_mapping": {
        "Item Reference 1": "reference_data",
        "Item Reference 2": "reference_data",
        "Item Reference 3": "reference_data",
        "Item Reference 4": "reference_data",
        "Item Amount": "amount",
        "Currency": "currency",
        "Item Type": "item_type",
        "Reconciliation": "reconciliation_group",
    },
    "reference_fields": ["reference_data"],
}


PROFILES = {
    "items_status": ITEMS_STATUS_PROFILE,
}