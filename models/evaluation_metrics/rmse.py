from pyspark.ml.evaluation import RegressionEvaluator

def calculate_rmse(predictions):
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    return rmse