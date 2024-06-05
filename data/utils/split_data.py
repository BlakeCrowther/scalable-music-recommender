from pyspark.sql.functions import rand, col, row_number
from pyspark.sql.window import Window

def user_based_train_test_split(data, num_test_ratings=10, total_percentage=1.0, seed=None):
    """
    Split the data into train and test sets such that there are at least
    a specified number of random ratings per user in the test set and
    the total combined count is limited to a percentage of the data.
    
    Ensures that if downsizing is needed, it maintains the ratings per test
    user constraint and downsizes from the train ratings.

    Args:
    - data (DataFrame): Input DataFrame containing 'user_id', 'song_id', and 'rating' columns.
    - num_test_ratings (int): The number of ratings to include in the test set per user.
    - total_percentage (float): The percentage of the data to include in the output.
    - seed (int or None): The seed for random operations to ensure reproducibility.

    Returns:
    - train_df (DataFrame): The training set DataFrame.
    - test_df (DataFrame): The test set DataFrame.
    """
    # Calculate the limit for the combined data
    total_limit = int(data.count() * total_percentage)

    # Shuffle the data
    shuffled_df = data.orderBy(rand(seed))

    # Assign row numbers within each user_id partition
    window = Window.partitionBy("user_id").orderBy(rand(seed))
    df_with_row_num = shuffled_df.withColumn("row_number", row_number().over(window))

    # Split into train and test sets
    test_df = df_with_row_num.filter(col("row_number") <= num_test_ratings).drop("row_number")
    train_df = df_with_row_num.filter(col("row_number") > num_test_ratings).drop("row_number")
    
    print(f"Initial train data count: {train_df.count()}")
    print(f"Initial test data count: {test_df.count()}")

    # Limit the total combined count (only limit the train set)
    total_train_count = total_limit - test_df.count()
    train_df = train_df.orderBy(rand(seed)).limit(total_train_count)
    
    # Print the count of records in train and test sets after limiting
    print(f"Final train data count: {train_df.count()}")
    print(f"Final test data count: {test_df.count()}")

    return train_df, test_df
