from pyspark.sql.functions import split, col
def cleaned_df(song_ratings, song_attributes, genre_hierarchy):
    song_ratings = song_ratings.withColumnRenamed("_c0", "user_id") \
        .withColumnRenamed("_c1", "song_id") \
        .withColumnRenamed("_c2", "rating") 
    song_attributes = song_attributes.withColumnRenamed("_c0", "song_id") \
        .withColumnRenamed("_c1", "album_id") \
        .withColumnRenamed("_c2", "artist_id") \
        .withColumnRenamed("_c3", "genre_id")

    genre_hierarchy = genre_hierarchy.withColumnRenamed("_c0", "genre_id") \
        .withColumnRenamed("_c1", "parent_genre_id") \
        .withColumnRenamed("_c2", "level") \
        .withColumnRenamed("_c3", "genre_name")

    df = song_ratings.join(song_attributes, "song_id", how="inner")
    df = df.join(genre_hierarchy,"genre_id", how="inner")

    df = df.drop('parent_genre_id').drop('level')
    df.describe().show()

    null_columns = {column: df.filter(col(column).isNull()).count() for column in df.columns}
    null_columns
    
    return df