from pyspark.ml import Transformer
from pyspark.sql.functions import col, expr, lit, row_number, when
from pyspark.sql.window import Window

class PrecisionAtKTransformer(Transformer):
    def __init__(self, k):
        self.k = k

    def _transform(self, dataset):
        # Add a relevance column based on the rating
        relevance_threshold = 4
        dataset = dataset.withColumn("relevance", when(col("rating") >= relevance_threshold, 1).otherwise(0))

        # Window specification to rank predictions within each user
        window_spec = Window.partitionBy("user_id").orderBy(col("prediction").desc())

        # Add a ranking column to order predictions by score within each user group
        ranked_predictions = dataset.withColumn("rank", row_number().over(window_spec))

        # Filter to get top K predictions for each user
        top_k_predictions = ranked_predictions.filter(col("rank") <= self.k)

        # Calculate the number of relevant items in the top K predictions
        num_relevant_and_recommended = top_k_predictions.filter(col("relevance") == 1).count()

        # Calculate the total number of recommendations made (i.e., K * number of users)
        num_users = dataset.select("user_id").distinct().count()
        total_recommendations = self.k * num_users

        # Calculate Precision@K
        precision_at_k = num_relevant_and_recommended / total_recommendations if total_recommendations > 0 else 0.0

        return dataset.withColumn("Precision@K", lit(precision_at_k))
