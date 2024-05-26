from pyspark.ml.classification import LogisticRegressionModel, RandomForestClassificationModel
from pyspark.ml.recommendation import ALSModel
\

MODEL_MAPPING = {
    "LogisticRegression": LogisticRegressionModel,
    "RandomForestClassifier": RandomForestClassificationModel,
    "ALS": ALSModel,
    # Add other models as needed
}

def load_model(model_type, model_path):
    """
    Load a PySpark model based on the model type string.

    Parameters:
    model_type (str): The type of the model to load (e.g., "LogisticRegression").
    model_path (str): The path to the saved model.

    Returns:
    model: The loaded PySpark model.
    """
    # Get the model class from the mapping dictionary
    model_class = MODEL_MAPPING.get(model_type)
    
    if model_class is None:
        raise ValueError(f"Unknown model type: {model_type}")
    
    # Load the model using the appropriate class
    model = model_class.load(model_path)
    return model
