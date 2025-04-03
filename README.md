## Amazon Review Data Analysis: Video Games on Amazon

### **Dataset:**
We will be using the Amazon Reviews 2023 dataset, specifically focusing on the "Video Games" category.

### **Dataset Download Link:**
[Amazon Reviews 2023](https://amazon-reviews-2023.github.io/)

### **Project Goal:**
To leverage Hadoop and Spark for cleaning, processing, and performing data visualization on the Amazon Video Games review dataset to extract valuable insights about the gaming industry on Amazon.

### **Steps:**
1. **Dataset Acquisition:**
    * Navigate to the provided link.
    * Locate the "Video Games" category within the dataset.
    * Download the relevant dataset files (likely in Parquet or JSON format, depending on the available options).

### **Project Structure:**

#### **Java Folder:**
Contains Hadoop components:
- **Reducer.java** - Implements the reducer logic.
- **Driver.java** - Manages job configuration and execution.
- **Mapper.java** - Handles the mapping phase for data processing.

#### **Python Scripts:**
- **dash_app.py** - Runs the front end of the visualization (requires high RAM usage).
- **ml.ipynb** - Machine learning and analytical tasks.
- **sentiment.ipynb** - Runs the visualization and sentiment analysis.
- **spark_codes.py** - Runs the visualization on the EMR (Elastic MapReduce) cluster.

This project integrates Hadoop, Spark, and Dash for efficient processing and visualization of Amazon Video Game reviews.

