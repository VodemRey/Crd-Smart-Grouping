from pathlib import Path
from loader import get_files, read_files
from dataset_detector import detect_profile
from normalize import data_normalize
 
base_path = Path(__file__).resolve().parent.parent

def main():


    input_path, key_values_path = get_files(base_path)
    entry_df, keys_df = read_files(input_path, key_values_path)

    profile_name = detect_profile(entry_df)

    normalized_df = data_normalize(entry_df, profile_name)

    for f in normalized_df.columns:
        print("\n",f)

if __name__ == "__main__":
    print(main())