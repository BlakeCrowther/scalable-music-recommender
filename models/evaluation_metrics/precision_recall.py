from pyspark.sql.functions import col, explode, lit, row_number, when
from pyspark.sql.window import Window

def calculate_precision_recall(recommendations, test_set, relevance_threshold):
    # Filter test set above the threshold
    test_set = test_set.withColumn('relevant', when(col('rating') >= relevance_threshold, 1).otherwise(0))
    filtered_test_data = test_set.filter(col('relevant') == 1)

    # Filter recommendations above the threshold
    filtered_recommendation_data = recommendations.withColumn('relevant', when(col('rating') >= relevance_threshold, 1).otherwise(0))
    filtered_recommendation_data = filtered_recommendation_data.filter(col('relevant') == 1)
    
    # Join the recommendations and test set 
    relevant_recommendations = filtered_recommendation_data.join(filtered_test_data, ['user_id', 'song_id'], 'inner')
    relevant_recommendations = relevant_recommendations.count()
    
    # Calculate precision
    total_recommendations = recommendations.count()
    precision = relevant_recommendations / total_recommendations if total_recommendations > 0 else 0.0
    print(f'Precision: {precision} = Relevant Recommendations: {relevant_recommendations} / Total Recommendations: {total_recommendations}')
    
    # Calculate recall
    relevant_test_items = filtered_test_data.count()
    recall = relevant_recommendations / relevant_test_items if relevant_test_items > 0 else 0.0
    print(f'Recall: {recall} = Relevant Recommendations: {relevant_recommendations} / Relevant Test Items: {relevant_test_items}')
    
    return precision, recall  
    
