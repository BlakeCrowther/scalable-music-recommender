from pyspark.ml.evaluation import RegressionEvaluator

def calculate_mae(predictions):
    evaluator = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")
    mae = evaluator.evaluate(predictions)
    return mae
