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

CRYSTAL_OPEN_ITEMS_PROFILE = {
    "name": "Crystal Open Items",
    "column_mapping": {
        "Ref1": "reference_data",
        "Ref2": "reference_data",
        "Ref3": "reference_data",
        "Ref4": "reference_data",
        "Original ID" : "reference_data",
        "Amount": "amount",
        "Entry type": "item_type",
        "Currency" : "currency",
        "Set ID": "reconciliation_group",
    },
    "reference_fileds":["reference_data"],
}

PROFILES = {
    "items_status": ITEMS_STATUS_PROFILE,
    "crystal_open_items": CRYSTAL_OPEN_ITEMS_PROFILE,
}