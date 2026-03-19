from pathlib import Path
from loader import get_files, read_files
from dataset_detector import detect_profile
from normalize import data_normalize
from issuers_assigner import assign_issuers
from grouping import prepare_grouping_dataframe, run_grouping_pipeline
from exporter import reorder_output_columns, sort_output, save_output

base_path = Path(__file__).resolve().parent.parent


def main():
    input_path, key_values_path = get_files(base_path)
    entry_df, keys_df = read_files(input_path, key_values_path)

    profile_name = detect_profile(entry_df)

    normalized_df, normalized_keys_df = data_normalize(entry_df, keys_df, profile_name)

    assigned_df = assign_issuers(normalized_df, normalized_keys_df)
    grouping_ready_df = prepare_grouping_dataframe(assigned_df, profile_name)
    grouped_df = run_grouping_pipeline(grouping_ready_df, profile_name)

    output_df = reorder_output_columns(grouped_df)
    output_df = sort_output(output_df)
    save_output(output_df, input_path, base_path)
    return output_df.head()

if __name__ == "__main__":
    print(main())