from pathlib import Path
from loader import get_files, read_files
from dataset_detector import detect_profile
from normalize import data_normalize
from issuers_assigner import assign_issuers
 
base_path = Path(__file__).resolve().parent.parent

def main():


    input_path, key_values_path = get_files(base_path)
    entry_df, keys_df = read_files(input_path, key_values_path)

    profile_name = detect_profile(entry_df)

    normalized_df, normalized_keys_df = data_normalize(entry_df, keys_df, profile_name)

    assigned_issers_df = assign_issuers(normalized_df, normalized_keys_df)

    return assigned_issers_df.head()

if __name__ == "__main__":
    print(main())