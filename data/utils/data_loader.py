from pyspark.sql import SparkSession, functions as F

HDFS_DATA = f"hdfs:///data"

def load_file_from_hdfs(file_name):
    hdfs_file_path = f"{HDFS_DATA}/{file_name}"
    spark = SparkSession.getActiveSession()
    return spark.read.csv(hdfs_file_path, sep="\t", header=False, inferSchema=True)

def load_from_hdfs(dataset, partition_number=0):
    train_file = f"{dataset}/train/train_{partition_number}.txt"
    test_file = f"{dataset}/test/test_{partition_number}.txt"

    train_data = load_file_from_hdfs(train_file)
    train_data = rename_and_reorder_columns(train_data)
    train_data = train_data.withColumn("partition_id", F.lit(partition_number))

    test_data = load_file_from_hdfs(test_file)
    test_data = rename_and_reorder_columns(test_data)
    test_data = test_data.withColumn("partition_id", F.lit(partition_number))

    print(f"Loaded partition {partition_number}: {train_data.count()} training records and {test_data.count()} test records from HDFS")
    train_data.printSchema()

    return train_data, test_data

# Load a single dataset file from HDFS and rename/reorder columns.
def load_data(file_path):
    data = load_file_from_hdfs(file_path)
    data = rename_and_reorder_columns(data)
    data.printSchema()
    print(f"Loaded {data.count()} records from HDFS")
    return data
    

def rename_and_reorder_columns(df):
    # Determine the number of columns in the DataFrame
    num_cols = len(df.columns)

    # Rename columns based on the number of columns
    if num_cols == 3:
        df = df.withColumnRenamed("_c0", "user_id") \
               .withColumnRenamed("_c1", "song_id") \
               .withColumnRenamed("_c2", "rating")
    elif num_cols == 7:
        df = df.withColumnRenamed("_c0", "user_id") \
               .withColumnRenamed("_c1", "song_id") \
               .withColumnRenamed("_c2", "rating") \
               .withColumnRenamed("_c3", "album_id") \
               .withColumnRenamed("_c4", "artist_id") \
               .withColumnRenamed("_c5", "genre_id") \
               .withColumnRenamed("_c6", "genre_name")
    else:
        raise ValueError("Unexpected number of columns in the dataset")
    
    # Enforce the desired order of columns
    if num_cols == 3:
        desired_order = ["user_id", "song_id", "rating"]
    elif num_cols == 7:
        desired_order = ["user_id", "song_id", "rating", "album_id", "artist_id", "genre_id", "genre_name"]
    
    df = df.select(desired_order)
    return df