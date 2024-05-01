# scaling-music-recommender

## Dataset

[Yahoo! Music User Ratings of Songs with Artist, Album, and Genre Meta Information, v. 1.0 (1.4 Gbyte & 1.1 Gbyte)](https://webscope.sandbox.yahoo.com/catalog.php?datatype=r&did=2)

- Each user rated at least 20 songs
- Each song rated by at least 20 users
- 10 Partitions of equally sized sets of users to enable cross-validation
- 10 user ratings in test with remaining 10+ ratings in training
- **Train `*/train-n.txt`**
  - `"user id<TAB>song id<TAB>rating"`
  - 200,000 users
  - Observations per user: 10+
  - Observations per user per song: 1
  - Observations per song
  - Rating: 1-5
- **Test: `*/test-n.txt`**
  - `"user id<TAB>song id<TAB>rating"`
  - 200,000 users
  - Observations per user: 10
- **Other**
  - `song-attributes.txt`: Each line of this file lists the attributes (artist id, album id, and genre id) for a particular song.
    - `song id<TAB>album id<TAB>artist id<TAB>genre id`
  - `genre-hierarchy.txt`: Each line of this file lists the id number for a genre, the id number of the parent of that genre, the level of the genre in the hierarchy, and the name of the genre.
    - `genre id<TAB>parent genre id<TAB>level<TAB>genre name`

## Considerations

- **Data Preparation**
  - Duplicates
  - Missing
  - Popularity Bias
  - Rating Inflation
  - Feature Engineering
- **Models/Techniques**
  - **Collaborative Filtering:**
    - **User-based Collaborative Filtering:** Recommend songs to a user based on the preferences of users who are similar to them.
      - **Similarity Scoring Techniques:** Cosine Similarity, Pearson Correlation Coefficient
    - **Song-based Collaborative Filtering:** Recommend songs to a user based on the similarities between songs they have rated highly in the past.
    - **Techniques:**
      - Matrix Factorization(Singular Value Decomposition(SVD), Alternating Least Squares (ALS)): Decompose the user-item interaction matrix into lower-d matrices to capture latent factors.
  - **Content-based Filtering:** Recommend songs to users based on the attributes or content of the items and the preferences of the users.
    - **Techniques:**
      - **Term Frequency-Inverse Document Frequency (TF-IDF):** Technique to represent the importance of each word (or attribute) in a document (or song).
        - It calculates a weight for each word based on its frequency in the song and across all songs in the dataset.
      - Word Embeddings
  - **Deep Learning Models:** Use neural network architecture to learn complex patterns from user-item interactions and attributes.
    - Can capture nonlinear relationships and higher-order interactions in the data.
    - **Techniques:**
      - **Singular Value Decomposition:** Matrix factorization technique that decomposes user-item interaction matrix into lower-dimensional matrices, capturing latent factors underlying the interactions.
      - **Recurrent Neural Networks (RNNs)**: Process sequential user behavior data, such as clickstream or session data.
      - **Convolutional Neural Networks (CNNs)**: Analyze item images or text descriptions to extract features for recommendation.
      - **Variational Autoencoders (VAEs)**: Learn probabilistic representations of users and items, allowing for more robust modeling of uncertainty and diversity in recommendations.
  - **Hybrid Models:** - Combine multiple recommendation algorithms (e.g., CF, content-based, deep learning) using **ensemble techniques** like blending, stacking, or weighted averaging to improve recommendation accuracy.
- **Evaluation Metrics**
  - Accuracy Metrics:
    - e.g. Accuracy of recommendation system to identify relevant songs for users based on ratings.
  - Ranking Metrics:
    - e.g. Average position of the first relevant song in the recommendation list for each user.
  - Diversity Metrics:
    - e.g. Catalog coverage measure of the proportion of unique songs in the entire catalog that are recommended to users.
  - Coverage Metrics
    - e.g. Proportion of users that receive recommendations.
