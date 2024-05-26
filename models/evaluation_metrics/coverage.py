from pyspark.sql.functions import countDistinct
    
def calculate_user_coverage(total_users, prediction_user_size):
    """
    Calculate user coverage.

    :param total_users: Total number of unique users in the train and test sets combined.
    :param prediction_user_size: Number of unique users in the prediction set.
    :return: User coverage ratio.
    """
    user_coverage = prediction_user_size / total_users if total_users > 0 else 0.0
    return user_coverage


def calculate_song_coverage(total_songs, prediction_song_size):
    """
    Calculate song coverage.

    :param total_songs: Total number of unique songs in the train and test sets combined.
    :param prediction_song_size: Number of unique songs in the prediction set.
    :return: Song coverage ratio.
    """
    song_coverage = prediction_song_size / total_songs if total_songs > 0 else 0.0
    return song_coverage

    