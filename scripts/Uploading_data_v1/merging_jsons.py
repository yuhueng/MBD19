import sys
from pyspark.sql import SparkSession

# Ensure the correct number of arguments is provided
if len(sys.argv) != 3:
    print("Usage: merge_json_files.py <input_path> <output_path>")
    sys.exit(1)

# Get paths from command-line arguments
readpath = sys.argv[1]
writepath = sys.argv[2]

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("Merge JSON Files") \
    .getOrCreate()

# Read the JSON files with the multiline option
df = spark.read.option("multiLine", "true").json(readpath)

# Write the DataFrame to a new .json.gz file with compression
df.write.mode('overwrite').option("compression", "gzip").json(writepath)

# Stop the Spark session
spark.stop()
