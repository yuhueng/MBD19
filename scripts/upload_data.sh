#!/bin/bash


# Start timing
start_time=$(date +%s)

years1=($(seq 1807 2 1859)) # 1859
#years2=($(seq 1861 2 1901))


for year in "${years1[@]}"; do
    # get the filename from s3355810
    filename="faro_${year}"
    # filename="faro_1799"

    echo "Start copying and untarring the file: $filename."
    hdfs dfs -get "/user/s3355810/americanstories_odd/$(basename "$filename").tar.gz" "/home/s2545705/americanstories"
    tar -xzvf "/home/s2545705/americanstories/$(basename "$filename").tar.gz" -C /home/s2545705/americanstories_unpacked/
    rm "/home/s2545705/americanstories/$(basename "$filename").tar.gz"
    echo "Done copying and untarring the $filename."

    LOCAL_DIR_ALL_JSONS="/home/s2545705/americanstories_unpacked/mnt/122a7683-fa4b-45dd-9f13-b18cc4f4a187/ca_rule_based_fa_clean/$filename"
    HDFS_DIR_ALL_JSONS="/user/s2545705/americanstories/$filename"
    HDFS_DIR_MERGED_JSONS="/user/s2545705/americanstories/merged/$(basename "$filename").json.gz"


    total_files=$(find "$LOCAL_DIR_ALL_JSONS" -type f -name "*.json" | wc -l)
    processed_files=0
    last_logged_percentage=-1

    for file in $LOCAL_DIR_ALL_JSONS/*.json; do
        if [ -f "$file" ]; then
            # Use a pipeline to gzip and upload directly to HDFS
            gzip -c "$file" | hdfs dfs -put - "$HDFS_DIR_ALL_JSONS/$(basename "$file").gz"

            # Update Messages
            processed_files=$((processed_files + 1))
            current_percentage=$((processed_files * 100 / total_files))

            # Log progress every 10% of the total files
            if (( current_percentage/5 > last_logged_percentage )); then
                echo "[$((processed_files * 100 / total_files))%] Progress: Uploaded $processed_files of $total_files files."
                last_logged_percentage=$((current_percentage/5))
            fi
        fi
    done
    echo "Upload complete! Processed $processed_files out of $total_files files."





    echo "Start merging jsons on the hdfs."
    spark-submit --conf spark.dynamicAllocation.maxExecutors=5 merging_jsons.py "$HDFS_DIR_ALL_JSONS" "$HDFS_DIR_MERGED_JSONS"
    echo "Done merging."

    echo "Start cleanup"
    rm -r "$LOCAL_DIR_ALL_JSONS"
    hdfs dfs -rm -r "$HDFS_DIR_ALL_JSONS"
    echo "Done cleanup"

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

