from pyspark.sql.functions import split, col

def cleaned_df(song_ratings, song_attributes, genre_hierarchy):
    # Rename Columns
    song_ratings = song_ratings.withColumnRenamed("_c0", "user_id") \
        .withColumnRenamed("_c1", "song_id") \
        .withColumnRenamed("_c2", "rating") 
        
    # Split and cast columns in song_attributes
    song_attributes = song_attributes.withColumn("song_id", split(song_attributes.value, "\t")[0]) \
        .withColumn("album_id", split(song_attributes.value, "\t")[1]) \
        .withColumn("artist_id", split(song_attributes.value, "\t")[2]) \
        .withColumn("genre_id", split(song_attributes.value, "\t")[3]) \
        .drop("value")
    song_attributes = song_attributes.withColumn("song_id", col("song_id").cast("integer")) \
        .withColumn("album_id", col("album_id").cast("integer")) \
        .withColumn("artist_id", col("artist_id").cast("integer"))

    # Split and cast columns in genre_hierarchy
    genre_hierarchy = genre_hierarchy.withColumn("genre_id", split(genre_hierarchy.value, "\t")[0]) \
        .withColumn("parent_genre_id", split(genre_hierarchy.value, "\t")[1]) \
        .withColumn("level", split(genre_hierarchy.value, "\t")[2]) \
        .withColumn("genre_name", split(genre_hierarchy.value, "\t")[3]) \
        .drop("value")
    genre_hierarchy = genre_hierarchy.withColumn("genre_id", col("genre_id").cast("integer")) \
        .withColumn("parent_genre_id", col("parent_genre_id").cast("integer")) \
        .withColumn("level", col("level").cast("integer"))
    song_attributes = song_attributes.withColumnRenamed("_c0", "song_id") \
        .withColumnRenamed("_c1", "album_id") \
        .withColumnRenamed("_c2", "artist_id") \
        .withColumnRenamed("_c3", "genre_id")

    genre_hierarchy = genre_hierarchy.withColumnRenamed("_c0", "genre_id") \
        .withColumnRenamed("_c1", "parent_genre_id") \
        .withColumnRenamed("_c2", "level") \
        .withColumnRenamed("_c3", "genre_name")

    # Perform Joins
    df = song_ratings.join(song_attributes, "song_id", how="inner")
    df = df.join(genre_hierarchy,"genre_id", how="inner")

    # Drop Unnecessary Columns
    df = df.drop('parent_genre_id').drop('level')
    
    # Enforce Column Order 
    df = df.select("user_id", "song_id", "rating", "album_id", "artist_id", "genre_id", "genre_name")
    
    # Describe Data    
    df.describe().show()

    null_columns = {column: df.filter(col(column).isNull()).count() for column in df.columns}
    print(f'Null Columns in cleaned df: {null_columns}')

    return df