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
        train_partition_df = train_partition_df.withColumnsRenamed({"_c0": "user_id", "_c1": "song_id", "_c2": "rating"})
        # Add a partition column
        train_partition_df = train_partition_df.withColumn("partition_id", F.lit(i))
        # Combine all the partitions
        if train_data is None:
            train_data = train_partition_df
        else:
            train_data = train_data.union(train_partition_df)
        
    for i, file in enumerate(test_files):
        test_partition_df = load_file_from_hdfs(file)
        test_partition_df = test_partition_df.withColumnsRenamed({"_c0": "user_id", "_c1": "song_id", "_c2": "rating"})
        # Add a partition column
        test_partition_df = test_partition_df.withColumn("partition_id", F.lit(i))
        # Comhbine all the partitions
        if test_data is None:
            test_data = test_partition_df
        else:
            test_data = test_data.union(test_partition_df)
    
    return train_data, test_data