from pyspark.sql.functions import split, col
def cleaned_df(song_ratings, song_attributes, genre_hierarchy):
    song_ratings = song_ratings.withColumnRenamed("_c0", "user_id") \
        .withColumnRenamed("_c1", "song_id") \
        .withColumnRenamed("_c2", "rating") 
    song_attributes = song_attributes.withColumn("song_id", split(song_attributes.value, "\t")[0]) \
       .withColumn("album_id", split(song_attributes.value, "\t")[1]) \
       .withColumn("artist_id", split(song_attributes.value, "\t")[2]) \
       .withColumn("genre_id", split(song_attributes.value, "\t")[3]) \
       .drop("value")
    song_attributes = song_attributes.withColumn("song_id", col("song_id").cast("integer")) \
       .withColumn("album_id", col("album_id").cast("integer")) \
       .withColumn("artist_id", col("artist_id").cast("integer"))
    genre_hierarchy = genre_hierarchy.withColumn("genre_id", split(genre_hierarchy.value, "\t")[0]) \
       .withColumn("parent_genre_id", split(genre_hierarchy.value, "\t")[1]) \
       .withColumn("level", split(genre_hierarchy.value, "\t")[2]) \
       .withColumn("genre_name", split(genre_hierarchy.value, "\t")[3]) \
       .drop("value")
    genre_hierarchy = genre_hierarchy.withColumn("genre_id", col("genre_id").cast("integer")) \
       .withColumn("parent_genre_id", col("parent_genre_id").cast("integer")) \
       .withColumn("level", col("level").cast("integer"))

    df = song_ratings.join(song_attributes, "song_id", how="inner")
    df = df.join(genre_hierarchy,"genre_id", how="inner")

    df = df.drop('parent_genre_id').drop('level')
    df.describe().show()

    null_columns = {column: df.filter(col(column).isNull()).count() for column in df.columns}
    null_columns
    
    return df