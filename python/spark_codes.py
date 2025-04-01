# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import json
import re
import html
from wordcloud import WordCloud, STOPWORDS
from datetime import datetime
import os

# Create output directory for saved visualizations
output_dir = "visualization_output"
os.makedirs(output_dir, exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Video Games Reviews Analysis") \
    .getOrCreate()

# FILE PATHS
partfilename = "/home/hadoop/part-r-00000"
metadata_filename = "/home/hadoop/meta_Video_Games.jsonl"
data_filename = "/home/hadoop/Video_Games_with_sentiment.jsonl"

# LOAD DATA
# Read the part file
lines_rdd = spark.sparkContext.textFile(partfilename)

# Parse the data
def parse_line(line):
    line = line.strip()
    keys, count = line.split("\t")
    year, month, sentiment, rating, asid = keys.split("-")
    
    return {
        "year": int(year),
        "month": month,
        "sentiment": sentiment,
        "rating": rating,
        "asid": asid,
        "count": int(count)
    }

# Transform RDD to DataFrame
data_rdd = lines_rdd.map(parse_line)
df = spark.createDataFrame(data_rdd)

# Display sample data (save to log output)
print("Sample data:")
df.show(5)

# Load metadata
metadata_schema = StructType([
    StructField("parent_asin", StringType(), True),
    StructField("title", StringType(), True),
    # Add other fields as needed
])

# Read metadata file
metadata_df = spark.read.json(metadata_filename)

# Create mapping: parent_asin → title
# Select only needed columns to create a lookup dataframe
title_map_df = metadata_df.select("parent_asin", "title")

# Map titles to review DataFrame based on 'asid'
df = df.join(title_map_df, df.asid == title_map_df.parent_asin, "left")

# Find top products
top_products = df.groupBy("title").agg(F.sum("count").alias("total_count")).orderBy(F.desc("total_count")).limit(5)
print("Top 5 products:")
top_products.show(truncate=False)

# Function to save figures to file
def save_figure(plt, filename, dpi=300):
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=dpi, bbox_inches='tight')
    print(f"Figure saved to {filepath}")
    plt.close()

# WORD CLOUD PREPARATION 
# Function to process review text for wordcloud
def get_review_text_spark(spark, filepath):
    # Read the JSONL file
    reviews_df = spark.read.json(filepath)
    
    # Filter for low ratings (less than 3)
    low_rating_reviews = reviews_df.filter(reviews_df.rating < 3)
    
    # Process the text
    low_rating_reviews = low_rating_reviews.withColumn(
        "clean_text", 
        F.regexp_replace(
            F.regexp_replace(
                F.lower(reviews_df.text), 
                r"<.*?>", " "
            ), 
            r"[^a-zA-Z0-9\s']", " "
        )
    )
    
    # Sample data for wordcloud if dataset is large
    # Adjust the fraction based on your dataset size
    sample_df = low_rating_reviews.sample(False, 0.1)
    
    # Collect texts locally for wordcloud
    texts = [row.clean_text for row in sample_df.select("clean_text").collect()]
    all_text = " ".join(texts)
    return all_text

# Same function for title text
def get_title_text_spark(spark, filepath):
    # Read the JSONL file
    reviews_df = spark.read.json(filepath)
    
    # Filter for low ratings (less than 3)
    low_rating_reviews = reviews_df.filter(reviews_df.rating < 3)
    
    # Process the title
    low_rating_reviews = low_rating_reviews.withColumn(
        "clean_title", 
        F.regexp_replace(
            F.regexp_replace(
                F.lower(reviews_df.title), 
                r"<.*?>", " "
            ), 
            r"[^a-zA-Z0-9\s']", " "
        )
    )
    
    # Sample data for wordcloud if dataset is large
    sample_df = low_rating_reviews.sample(False, 0.1)
    
    # Collect titles locally for wordcloud
    texts = [row.clean_title for row in sample_df.select("clean_title").collect()]
    all_text = " ".join(texts)
    return all_text

# Function to create and save wordcloud
def create_and_save_wordcloud(all_text, title, filename):
    # Generate the word cloud
    stopwords = set(STOPWORDS)

    # Generate the word cloud
    wordcloud = WordCloud(
        width=1000,
        height=600,
        background_color="white",
        stopwords=stopwords
    ).generate(all_text)

    # Create figure for the word cloud
    plt.figure(figsize=(12, 6))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    
    # Save the figure
    save_figure(plt, filename)
    print(f"Wordcloud '{title}' saved as {filename}")

# Get review text and create wordcloud
print("Generating review text wordcloud...")
review_text = get_review_text_spark(spark, data_filename)
create_and_save_wordcloud(review_text, "Review Text", "review_wordcloud.png")

# Get title text and create wordcloud
print("Generating title text wordcloud...")
title_text = get_title_text_spark(spark, data_filename)
create_and_save_wordcloud(title_text, "Title Text", "title_wordcloud.png")

# RATING PIE CHART
print("Generating rating distribution pie chart...")
rating_counts = df.groupBy("rating").agg(F.sum("count").alias("total")).orderBy("rating")
rating_data = rating_counts.collect()

# Convert to lists for plotting
ratings = [row.rating for row in rating_data]
counts = [row.total for row in rating_data]

# Plot pie chart
plt.figure(figsize=(8, 8))
plt.pie(counts, labels=ratings, autopct="%1.1f%%", startangle=140)
plt.title("Rating Distribution")
plt.axis("equal")

# Save the figure
save_figure(plt, "rating_distribution.png")

# SENTIMENT PIE CHART
print("Generating sentiment distribution pie chart...")
sentiment_counts = df.groupBy("sentiment").agg(F.sum("count").alias("total")).orderBy("sentiment")
sentiment_data = sentiment_counts.collect()

# Convert to lists for plotting
sentiments = [row.sentiment for row in sentiment_data]
counts = [row.total for row in sentiment_data]

# Plot pie chart
plt.figure(figsize=(8, 8))
wedges, texts, autotexts = plt.pie(
    counts,
    labels=sentiments,
    autopct="%1.1f%%",
    startangle=140
)

plt.title("Sentiment Distribution")
plt.axis("equal")

# Add legend
plt.legend(wedges, sentiments, title="Sentiment", loc="lower right")

# Save the figure
save_figure(plt, "sentiment_distribution.png")

# BAR CHART
print("Generating product review count bar charts...")
# Group by title and filter by minimum review count
product_counts = df.groupBy("title").agg(F.sum("count").alias("total"))
filtered_counts = product_counts.filter(F.col("total") >= 1000)

# Get top and bottom 10 products
top_10 = filtered_counts.orderBy(F.desc("total")).limit(10)
bottom_10 = filtered_counts.orderBy("total").limit(10)

# Convert to pandas for easier plotting (this is a small dataset at this point)
top_10_pd = top_10.toPandas()
bottom_10_pd = bottom_10.toPandas()

# Create subplot with 2 rows and 1 column
fig, axes = plt.subplots(2, 1, figsize=(12, 12))

# Top 10 - Horizontal Bar Chart
axes[0].barh(top_10_pd["title"].iloc[::-1], top_10_pd["total"].iloc[::-1])  # Reverse for highest at top
axes[0].set_title("Top 10 Products by Review Count")
axes[0].set_xlabel("Review Count")
axes[0].set_ylabel("Product Title")

# Bottom 10 - Horizontal Bar Chart
axes[1].barh(bottom_10_pd["title"], bottom_10_pd["total"], color="orange")
axes[1].set_title("Bottom 10 Products by Review Count")
axes[1].set_xlabel("Review Count")
axes[1].set_ylabel("Product Title")

# Layout and save
plt.tight_layout()
save_figure(plt, "product_review_counts.png")

# YEARLY SALES
print("Generating yearly sales trend chart...")
yearly_sales = df.groupBy("year").agg(F.sum("count").alias("total")).orderBy("year")
yearly_data = yearly_sales.collect()

# Convert to lists for plotting
years = [row.year for row in yearly_data]
sales = [row.total for row in yearly_data]

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(years, sales, marker='o', linestyle='-', linewidth=2)

# Labels and Title
plt.title("Change in Review Counts (as Sales Proxy) from 2010 to 2023")
plt.xlabel("Year")
plt.ylabel("Total Review Count")
plt.grid(True)

# Save the plot
save_figure(plt, "yearly_sales_trend.png")

# RATING TRENDS
print("Generating rating trends over time chart...")
# Group by year and rating, then sum the counts
rating_trends = df.groupBy("year", "rating").agg(F.sum("count").alias("total")).orderBy("year", "rating")

# Convert to pandas for easier plotting
rating_trends_pd = rating_trends.toPandas()

# Pivot the data for plotting
pivot_data = rating_trends_pd.pivot(index="year", columns="rating", values="total")

# Plotting
plt.figure(figsize=(12, 6))
for rating in pivot_data.columns:
    plt.plot(pivot_data.index, pivot_data[rating], marker='o', label=rating)

# Labels and Title
plt.title("Rating Trends Over Time")
plt.xlabel("Year")
plt.ylabel("Review Count")
plt.legend(title="Rating")
plt.grid(True)
plt.tight_layout()

# Save the plot
save_figure(plt, "rating_trends.png")

# XBOX/SONY/PLAYSTATION
print("Generating brand mentions over time chart...")
# Define a schema for the reviews
review_schema = StructType([
    StructField("rating", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# Read the full reviews data
reviews_df = spark.read.json(data_filename)

# Process the data
reviews_df = reviews_df.withColumn(
    "year", F.year(F.from_unixtime(F.col("timestamp") / 1000))
).withColumn(
    "month", F.month(F.from_unixtime(F.col("timestamp") / 1000))
)

# Add platform mention flags
reviews_df = reviews_df.withColumn(
    "combined_text", F.concat_ws(" ", F.lower(F.col("title")), F.lower(F.col("text")))
).withColumn(
    "mentions_xbox", F.array_contains(F.split(F.col("combined_text"), " "), "microsoft")
).withColumn(
    "mentions_nintendo", F.array_contains(F.split(F.col("combined_text"), " "), "nintendo")
).withColumn(
    "mentions_sony", F.array_contains(F.split(F.col("combined_text"), " "), "sony")
).withColumn(
    "mentions_controller", F.array_contains(F.split(F.col("combined_text"), " "), "controller")
)

# Create a date column
reviews_df = reviews_df.withColumn(
    "date", F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01")))
)

# Group by year-month for each platform
xbox_counts = reviews_df.filter(F.col("mentions_xbox") == True) \
    .groupBy(F.year("date"), F.month("date")) \
    .count() \
    .withColumnRenamed("count", "xbox_count") \
    .withColumn("year_month", F.concat_ws("-", F.col("year(date)"), F.col("month(date)")))

nintendo_counts = reviews_df.filter(F.col("mentions_nintendo") == True) \
    .groupBy(F.year("date"), F.month("date")) \
    .count() \
    .withColumnRenamed("count", "nintendo_count") \
    .withColumn("year_month", F.concat_ws("-", F.col("year(date)"), F.col("month(date)")))

sony_counts = reviews_df.filter(F.col("mentions_sony") == True) \
    .groupBy(F.year("date"), F.month("date")) \
    .count() \
    .withColumnRenamed("count", "sony_count") \
    .withColumn("year_month", F.concat_ws("-", F.col("year(date)"), F.col("month(date)")))

# Join all platforms data
all_platforms = xbox_counts.join(
    nintendo_counts, 
    ["year(date)", "month(date)", "year_month"], 
    "outer"
).join(
    sony_counts, 
    ["year(date)", "month(date)", "year_month"], 
    "outer"
)

# Fill null values with 0
all_platforms = all_platforms.fillna(0)

# Convert to pandas for plotting
platforms_pd = all_platforms.select("year_month", "xbox_count", "nintendo_count", "sony_count").orderBy("year_month").toPandas()

# Plot the data
plt.figure(figsize=(14, 7))
plt.plot(platforms_pd["year_month"], platforms_pd["xbox_count"], label="Xbox", color="green")
plt.plot(platforms_pd["year_month"], platforms_pd["nintendo_count"], label="Nintendo", color="red")
plt.plot(platforms_pd["year_month"], platforms_pd["sony_count"], label="Sony", color="blue")

plt.title("Brand Mentions by Month")
plt.xlabel("Month")
plt.ylabel("Number of Mentions")
plt.grid(True)
plt.legend()

# Make x-axis labels more readable
plt.xticks(
    ticks=range(0, len(platforms_pd), 6),  # Every 6 months
    labels=platforms_pd["year_month"].iloc[::6],
    rotation=45
)

plt.tight_layout()

# Save the plot
save_figure(plt, "brand_mentions.png")

# Generate an HTML report of all visualizations
html_output = f"""<!DOCTYPE html>
<html>
<head>
    <title>Video Games Review Analysis Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333366; }}
        h2 {{ color: #666699; margin-top: 30px; }}
        img {{ max-width: 800px; border: 1px solid #ddd; margin: 10px 0; }}
        .section {{ margin-bottom: 40px; }}
    </style>
</head>
<body>
    <h1>Video Games Review Analysis Report</h1>
    
    <div class="section">
        <h2>Top Products</h2>
        <p>Top 5 products by review count:</p>
        <pre>{top_products.toPandas().to_html()}</pre>
    </div>
    
    <div class="section">
        <h2>Word Clouds</h2>
        <p>Word cloud from review text (negative reviews):</p>
        <img src="review_wordcloud.png" alt="Review Text Word Cloud">
        
        <p>Word cloud from review titles (negative reviews):</p>
        <img src="title_wordcloud.png" alt="Title Text Word Cloud">
    </div>
    
    <div class="section">
        <h2>Rating and Sentiment Distribution</h2>
        <p>Distribution of ratings:</p>
        <img src="rating_distribution.png" alt="Rating Distribution">
        
        <p>Distribution of sentiment:</p>
        <img src="sentiment_distribution.png" alt="Sentiment Distribution">
    </div>
    
    <div class="section">
        <h2>Product Review Counts</h2>
        <p>Top and bottom products by review count:</p>
        <img src="product_review_counts.png" alt="Product Review Counts">
    </div>
    
    <div class="section">
        <h2>Yearly Trends</h2>
        <p>Review count trend by year:</p>
        <img src="yearly_sales_trend.png" alt="Yearly Sales Trend">
        
        <p>Rating trends over time:</p>
        <img src="rating_trends.png" alt="Rating Trends">
    </div>
    
    <div class="section">
        <h2>Brand Mentions</h2>
        <p>Brand mentions over time:</p>
        <img src="brand_mentions.png" alt="Brand Mentions">
    </div>
</body>
</html>
"""

# Save the HTML report
html_path = os.path.join(output_dir, "analysis_report.html")
with open(html_path, 'w') as f:
    f.write(html_output)
print(f"HTML report saved to {html_path}")

print(f"All visualizations saved to {output_dir}/")
print("Analysis complete!")

# Stop the Spark session when done
spark.stop()