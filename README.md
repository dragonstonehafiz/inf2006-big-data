## Amazon Review Data Analysis: Video Games on Amazon

**Dataset:**

We will be using the Amazon Reviews 2023 dataset, specifically focusing on the "Video Games" category.

**Dataset Download Link:**

[https://amazon-reviews-2023.github.io/](https://amazon-reviews-2023.github.io/)

**Project Goal:**

To leverage Hadoop and Spark for cleaning, processing, and performing data visualization on the Amazon Video Games review dataset to extract valuable insights about the gaming industry on Amazon.

**Steps:**

1.  **Dataset Acquisition:**
    * Navigate to the provided link.
    * Locate the "Video Games" category within the dataset.
    * Download the relevant dataset files (likely in Parquet or JSON format, depending on the available options).

2.  **Hadoop Distributed File System (HDFS) Setup:**
    * Configure and set up a Hadoop cluster.
    * Transfer the downloaded dataset files to HDFS.

3.  **Data Cleaning and Preprocessing with Spark:**
    * Utilize Spark's DataFrame API for efficient data manipulation.
    * Perform the following cleaning tasks:
        * Handle missing values (imputation or removal).
        * Remove duplicate reviews.
        * Clean and standardize text data (e.g., lowercase, remove punctuation, tokenization).
        * Convert data types as necessary.
        * Extract relevant features from the review text.

4.  **Data Processing and Feature Engineering:**
    * Perform sentiment analysis on review text using NLP libraries (e.g., Spark NLP or TextBlob).
    * Calculate review statistics (e.g., average rating, review length, number of reviews per product).
    * Aggregate data to analyze trends (e.g., ratings over time, popular keywords).

5.  **Data Visualization:**
    * Use Spark's capabilities to aggregate and prepare data for visualization.
    * Utilize visualization libraries like Matplotlib, Seaborn, or Plotly (with data exported from Spark to Pandas DataFrames) for creating insightful visualizations.
    * Create visualizations to:
        * Show rating distributions.
        * Visualize sentiment trends.
        * Identify popular products and trends.
        * Display word clouds of frequent terms.
        * Show ratings over time.

6.  **Insights and Analysis:**
    * Analyze the visualizations to extract meaningful insights about the video game industry on Amazon.
    * Identify popular games, customer preferences, and potential areas for improvement.
    * Determine the influence of review sentiment on product rating.
    * Determine trends in gaming related vocabulary over time.

