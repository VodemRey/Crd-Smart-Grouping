import pandas as pd

entry_path = "data/Entry.xlsx"
key_path = "data/Key values.xlsx"

entry_df = pd.read_excel(entry_path)
key_df = pd.read_excel(key_path)