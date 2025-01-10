import json
import os


def merge_json_files(input_directory, output_file_directory, filename):
    """
    Merges all JSON files in the input_directory into output_files of maximum 37 MB.

    Args:
        :param input_directory: Directory containing JSON files to merge. (str)
        :param output_file_directory: Path to the output JSON file. (str)
        :param filename: The file name. (str)
    """
    merged_data = []
    MAX_OUTPUT_FILE_SIZE_MB = 36

    # List all JSON files in the directory
    json_files = [f for f in os.listdir(input_directory) if f.endswith('.json')]

    output_file_index = 1
    current_output_file = f"{output_file_directory}/{filename}_{output_file_index}.json"


    total_files = json_files.__len__()
    processed_files = 0
    last_logged_percentage = -1

    with open(current_output_file, 'w') as outfile:
        outfile.write('[')  # Start of JSON array
        first_file = True

        for json_file in json_files:
            file_path = os.path.join(input_directory, json_file)

            output_file_size = os.path.getsize(current_output_file) / (1024 * 1024)
            if output_file_size > MAX_OUTPUT_FILE_SIZE_MB:
                outfile.write(']')  # Close current JSON array
                outfile.close()
                output_file_index += 1
                current_output_file = f"{output_file_directory}/{filename}_{output_file_index}.json"
                outfile = open(current_output_file, 'w')
                outfile.write('[')
                first_file = True  # Reset first file flag for new file
                print(f"File: {current_output_file} created" )

            # Read each JSON file
            with open(file_path, 'r') as infile:
                try:
                    data = json.load(infile)  # Parse the JSON
                    if not first_file:
                        outfile.write(',')  # Add a comma to separate JSON objects
                    json.dump(data, outfile)  # Append data to the output file
                    first_file = False
                except json.JSONDecodeError:
                    print(f"Error decoding {json_file}, skipping.")

            processed_files = processed_files + 1
            current_percentage = (processed_files * 100 / total_files)

            if int(current_percentage) // 25 > last_logged_percentage:
                print(
                    f"[{int((processed_files * 100 / total_files))}%] Progress: Merged {processed_files} of {total_files} files.")
                last_logged_percentage = (current_percentage // 25)

        outfile.write(']')  # End of JSON array


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python3 merge_json.py <input_directory> <output_file_directory> <output_file_name>")
    else:
        print("Start merging")
        merge_json_files(sys.argv[1], sys.argv[2], sys.argv[3])
        print("Done merging")