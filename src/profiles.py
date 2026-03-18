"""Input profile mappings for supported datasets.

Profiles define how to interpret and normalize spreadsheet columns
for each supported dataset type. Use this module to configure
column mappings and reference field definitions.
"""


ITEMS_STATUS_PROFILE = {
    "name": "Items Status",
    "column_mapping": {
        "Item Reference 1": "gr_reference_data",
        "Item Reference 2": "gr_reference_data",
        "Item Reference 3": "gr_reference_data",
        "Item Reference 4": "gr_reference_data",
        "Item Amount": "gr_amount",
        "Currency": "gr_currency",
        "Item Type": "gr_item_type",
        "Reconciliation": "gr_reconciliation_group",
    },
    "reference_fields": ["gr_reference_data"],
}

CRYSTAL_OPEN_ITEMS_PROFILE = {
    "name": "Crystal Open Items",
    "column_mapping": {
        "Ref1": "gr_reference_data",
        "Ref2": "gr_reference_data",
        "Ref3": "gr_reference_data",
        "Ref4": "gr_reference_data",
        "Original ID": "gr_reference_data",
        "Amount": "gr_amount",
        "Entry type": "gr_item_type",
        "Currency": "gr_currency",
        "Set ID": "gr_reconciliation_group",
    },
    "reference_fields": ["gr_reference_data"],
}

PROFILES = {
    "items_status": ITEMS_STATUS_PROFILE,
    "crystal_open_items": CRYSTAL_OPEN_ITEMS_PROFILE,
}