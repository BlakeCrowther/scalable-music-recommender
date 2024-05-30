from pyspark.sql import SparkSession, functions as F

HDFS_DATA = f"hdfs:///data"

def load_file_from_hdfs(file_name):
    hdfs_file_path = f"{HDFS_DATA}/{file_name}"
    spark = SparkSession.getActiveSession()
    return spark.read.csv(hdfs_file_path, sep="\t", header=False, inferSchema=True)

def load_from_hdfs(dataset, partitions=10):
    train_files = [f"{dataset}/train/train_{i}.txt" for i in range(partitions)]
    test_files = [f"{dataset}/test/test_{i}.txt" for i in range(partitions)]

    train_data = None
    test_data = None
    for i, file in enumerate(train_files):
        train_partition_df = load_file_from_hdfs(file)
        train_partition_df = rename_and_reorder_columns(train_partition_df)
        # Add a partition column
        train_partition_df = train_partition_df.withColumn("partition_id", F.lit(i))
        # Combine all the partitions
        if train_data is None:
            train_data = train_partition_df
        else:
            train_data = train_data.union(train_partition_df)
        
    for i, file in enumerate(test_files):
        test_partition_df = load_file_from_hdfs(file)
        test_partition_df = rename_and_reorder_columns(test_partition_df)
        # Add a partition column
        test_partition_df = test_partition_df.withColumn("partition_id", F.lit(i))
        # Combine all the partitions
        if test_data is None:
            test_data = test_partition_df
        else:
            test_data = test_data.union(test_partition_df)
    
    print(f"Loaded {train_data.count()} training records and {test_data.count()} test records from HDFS")
    train_data.printSchema()
    
    return train_data, test_data


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