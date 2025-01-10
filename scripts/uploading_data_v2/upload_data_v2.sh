#!/bin/bash

# Start timing
start_time=$(date +%s)

years=($(seq 1871 2 1901))

for year in "${years[@]}"; do
  # get the filename from s3355810
  filename="faro_${year}"
  #filename="faro_1857"

  echo "Start copying and untarring the file: $filename."
  hdfs dfs -get "/user/s3355810/americanstories_odd/$(basename "$filename").tar.gz" "/home/s2545705/americanstories"
  tar -xzvf "/home/s2545705/americanstories/$(basename "$filename").tar.gz" -C /home/s2545705/americanstories_unpacked/
  rm "/home/s2545705/americanstories/$(basename "$filename").tar.gz"
  echo "Done copying and untarring the $filename."

  LOCAL_DIR_ALL_JSONS="/home/s2545705/americanstories_unpacked/mnt/122a7683-fa4b-45dd-9f13-b18cc4f4a187/ca_rule_based_fa_clean/$filename"
  LOCAL_DIR_MERGED_JSONS="/home/s2545705/americanstories/$filename"
  HDFS_DIR_MERGED_JSONS="/user/s2545705/americanstories/merged/$(basename "$filename").json.gz"

  mkdir "$LOCAL_DIR_MERGED_JSONS"
  python3 local_json_merge.py "$LOCAL_DIR_ALL_JSONS" "$LOCAL_DIR_MERGED_JSONS" "$filename"


  total_files=$(find "$LOCAL_DIR_MERGED_JSONS" -type f -name "*.json" | wc -l)
  processed_files=0
  last_logged_percentage=-1

  echo "Start uploading to HDFS"
  for file in $LOCAL_DIR_MERGED_JSONS/*.json; do
    if [ -f "$file" ]; then
      # Use a pipeline to gzip and upload directly to HDFS
      gzip -c "$file" | hdfs dfs -put - "$HDFS_DIR_MERGED_JSONS/$(basename "$file").gz"
      echo "Uploaded $file"
      # Update Messages
      processed_files=$((processed_files + 1))
      current_percentage=$((processed_files * 100 / total_files))

      # Log progress every 5% of the total files
      if ((current_percentage / 5 > last_logged_percentage)); then
        echo "[$((processed_files * 100 / total_files))%] Progress: Uploaded $processed_files of $total_files files."
        last_logged_percentage=$((current_percentage / 5))
      fi

    fi
  done
  echo "Upload complete! Processed all files."

  echo "Cleanup"
  rm -r "$LOCAL_DIR_ALL_JSONS"
  rm -r "$LOCAL_DIR_MERGED_JSONS"

  # End timing
  end_time=$(date +%s)

  # Calculate the time taken
  elapsed_time=$((end_time - start_time))
  # Calculate minutes and seconds
  minutes=$((elapsed_time / 60))
  seconds=$((elapsed_time % 60))

  # Display the elapsed time in minutes and seconds
  echo "Time taken: ${minutes}m ${seconds}s"
done
