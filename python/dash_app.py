import pandas as pd
import json
import re
import html
from dash import Dash, dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud, STOPWORDS
import numpy as np
import base64
from io import BytesIO
from datetime import datetime

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])

# Define the file paths
partfilename = "data/part-r-00000"
metadata_filename = "data/meta_Video_Games.jsonl"
data_filename = "data/Video_Games_with_sentiment.jsonl"

# Load the aggregated data
def load_aggregated_data():
    data = []
    with open(partfilename) as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            keys, count = line.split("\t")
            year, month, sentiment, rating, asid = keys.split("-")
            
            # Add to list as a dictionary
            data.append({
                "year": int(year),
                "month": month,
                "sentiment": sentiment,
                "rating": rating,
                "asid": asid,
                "count": int(count)
            })
    
    # Create DataFrame from the list of dicts
    df = pd.DataFrame(data)
    
    # Load product metadata JSONL
    with open(metadata_filename, "r") as f:
        metadata = [json.loads(line) for line in f]
    
    # Create mapping: parent_asin → title
    title_map = {item["parent_asin"]: item["title"] for item in metadata}
    
    # Map titles to review DataFrame based on 'asid'
    df["title"] = df["asid"].map(title_map)
    
    return df

# Load the detailed review data for the wordclouds and brand mentions
def load_detailed_data():
    data = []
    with open(data_filename, "r", encoding="utf-8") as file:
        lines = file.readlines()
    
    for line in lines:
        try:
            review = json.loads(line)
            
            # Extract and process fields
            rating = review.get("rating")
            title_text = review.get("title", "").lower()
            review_text = review.get("text", "").lower()
            sentiment = review.get("sentiment")
            asin = review.get("asin")
            timestamp = review.get("timestamp")
            
            # Convert timestamp to year and month
            if timestamp:
                dt = datetime.fromtimestamp(timestamp / 1000)
                year = dt.year
                month = dt.month
            else:
                year = None
                month = None
            
            # Combine title and text for keyword search
            combined_text = title_text + " " + review_text
            
            # Detect mentions
            mentions_xbox = any(kw in combined_text for kw in ["microsoft", "xbox"])
            mentions_nintendo = any(kw in combined_text for kw in ["nintendo", "switch"])
            mentions_sony = any(kw in combined_text for kw in ["sony", "playstation"])
            mentions_controller = any(kw in combined_text for kw in ["controller"])
            mentions_halo = any(kw in combined_text for kw in ["halo"])
            
            data.append({
                "year": year,
                "month": month,
                "rating": rating,
                "sentiment": sentiment,
                "asin": asin,
                "title_text": title_text,
                "review_text": review_text,
                "mentions_xbox": mentions_xbox,
                "mentions_nintendo": mentions_nintendo,
                "mentions_sony": mentions_sony,
                "mentions_controller": mentions_controller,
                "mentions_halo": mentions_halo,
            })
        except:
            continue  # skip malformed lines
    
    # Convert to DataFrame
    detailed_df = pd.DataFrame(data)
    return detailed_df

# Create wordclouds
def create_wordcloud(text_data, max_words=100):
    # Generate the word cloud
    stopwords = set(STOPWORDS)
    wordcloud = WordCloud(
        width=800,
        height=400,
        background_color="white",
        max_words=max_words,
        stopwords=stopwords
    ).generate(text_data)
    
    # Convert the image to a format that can be displayed in Dash
    img = wordcloud.to_image()
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    img_str = base64.b64encode(buffer.getvalue()).decode()
    
    return img_str

# Function to get review text based on rating filter
def get_filtered_review_text(df, min_rating=1, max_rating=5, text_field="review_text"):
    filtered_df = df[(df["rating"] >= min_rating) & (df["rating"] <= max_rating)]
    
    texts = []
    for _, row in filtered_df.iterrows():
        text = row[text_field]
        if isinstance(text, str):
            clean_text = html.unescape(text)
            clean_text = re.sub(r"<.*?>", " ", clean_text)
            clean_text = re.sub(r"[^a-zA-Z0-9\s']", " ", clean_text).lower()
            tokens = clean_text.split()
            tokens = [word for word in tokens if len(word) > 1 or word in ("i", "a")]
            texts.append(" ".join(tokens))
    
    all_text = " ".join(texts)
    return all_text

# Load the data
df = load_aggregated_data()
detailed_df = load_detailed_data()

# Create date columns for detailed data
detailed_df["date"] = pd.to_datetime(detailed_df[["year", "month"]].assign(day=1))
detailed_df["year_month"] = detailed_df["date"].dt.to_period("M")

# Get unique years for dropdown
years = sorted(df["year"].unique())

# Layout of the app
app.layout = html.Div([
    html.H1("Video Games Reviews Dashboard", style={'textAlign': 'center'}),
    
    html.Div([
        html.Div([
            html.H3("Filters", style={'textAlign': 'center'}),
            html.Label("Select Year Range:"),
            dcc.RangeSlider(
                id='year-slider',
                min=min(years),
                max=max(years),
                value=[min(years), max(years)],
                marks={str(year): str(year) for year in years},
                step=1
            ),
            
            html.Label("Select Rating:"),
            dcc.RangeSlider(
                id='rating-slider',
                min=1,
                max=5,
                value=[1, 5],
                marks={i: str(i) for i in range(1, 6)},
                step=1
            ),
        ], style={'padding': '20px', 'flex': '1'}),
    ]),
    
    html.Div([
        html.Div([
            html.H3("Rating Distribution", style={'textAlign': 'center'}),
            dcc.Graph(id='rating-pie-chart')
        ], style={'width': '50%', 'display': 'inline-block'}),
        
        html.Div([
            html.H3("Sentiment Distribution", style={'textAlign': 'center'}),
            dcc.Graph(id='sentiment-pie-chart')
        ], style={'width': '50%', 'display': 'inline-block'})
    ]),
    
    html.Div([
        html.H3("Review Count Trends Over Time", style={'textAlign': 'center'}),
        dcc.Graph(id='yearly-trend-chart')
    ]),
    
    html.Div([
        html.H3("Rating Trends Over Time", style={'textAlign': 'center'}),
        dcc.Graph(id='rating-trends-chart')
    ]),
    
    html.Div([
        html.H3("Top Products by Review Count", style={'textAlign': 'center'}),
        dcc.Graph(id='top-products-chart')
    ]),
    
    html.Div([
        html.H3("Brand Mentions Over Time", style={'textAlign': 'center'}),
        dcc.Graph(id='brand-mentions-chart')
    ]),
    
    html.Div([
        html.H3("Controller Mentions by Sentiment", style={'textAlign': 'center'}),
        dcc.Graph(id='controller-sentiment-chart')
    ]),
    
    html.Div([
        html.H3("Word Cloud - Review Text", style={'textAlign': 'center'}),
        html.Img(id='wordcloud-image', style={'width': '100%'})
    ]),
    
    html.Div([
        html.H3("Word Cloud - Review Titles", style={'textAlign': 'center'}),
        html.Img(id='wordcloud-title-image', style={'width': '100%'})
    ]),
    
], style={'maxWidth': '1200px', 'margin': '0 auto'})

# Callback for the rating pie chart
@callback(
    Output('rating-pie-chart', 'figure'),
    Input('year-slider', 'value')
)
def update_rating_pie(years_range):
    filtered_df = df[(df["year"] >= years_range[0]) & (df["year"] <= years_range[1])]
    rating_counts = filtered_df.groupby("rating")["count"].sum().reset_index()
    
    fig = px.pie(
        rating_counts, 
        values='count', 
        names='rating', 
        title="Rating Distribution",
        color_discrete_sequence=px.colors.sequential.Blues
    )
    
    fig.update_traces(textinfo='percent+label')
    return fig

# Callback for the sentiment pie chart
@callback(
    Output('sentiment-pie-chart', 'figure'),
    Input('year-slider', 'value')
)
def update_sentiment_pie(years_range):
    filtered_df = df[(df["year"] >= years_range[0]) & (df["year"] <= years_range[1])]
    sentiment_counts = filtered_df.groupby("sentiment")["count"].sum().reset_index()
    
    fig = px.pie(
        sentiment_counts, 
        values='count', 
        names='sentiment', 
        title="Sentiment Distribution",
        color_discrete_sequence=px.colors.sequential.Greens
    )
    
    fig.update_traces(textinfo='percent+label')
    return fig

# Callback for the yearly trend chart
@callback(
    Output('yearly-trend-chart', 'figure'),
    Input('year-slider', 'value')
)
def update_yearly_trend(years_range):
    filtered_df = df[(df["year"] >= years_range[0]) & (df["year"] <= years_range[1])]
    yearly_counts = filtered_df.groupby("year")["count"].sum().reset_index()
    
    fig = px.line(
        yearly_counts, 
        x="year", 
        y="count", 
        title="Review Count by Year",
        markers=True
    )
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Review Count"
    )
    
    return fig

# Callback for the rating trends chart
@callback(
    Output('rating-trends-chart', 'figure'),
    Input('year-slider', 'value')
)
def update_rating_trends(years_range):
    filtered_df = df[(df["year"] >= years_range[0]) & (df["year"] <= years_range[1])]
    rating_trends = filtered_df.groupby(["year", "rating"])["count"].sum().reset_index()
    
    fig = px.line(
        rating_trends, 
        x="year", 
        y="count", 
        color="rating",
        title="Rating Trends Over Time",
        markers=True
    )
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Review Count",
        legend_title="Rating"
    )
    
    return fig

# Callback for the top products chart
@callback(
    Output('top-products-chart', 'figure'),
    [Input('year-slider', 'value'), Input('rating-slider', 'value')]
)
def update_top_products(years_range, rating_range):
    filtered_df = df[
        (df["year"] >= years_range[0]) & 
        (df["year"] <= years_range[1]) &
        (df["rating"] >= str(rating_range[0])) & 
        (df["rating"] <= str(rating_range[1]))
    ]
    
    product_counts = filtered_df.groupby("title")["count"].sum().reset_index()
    product_counts = product_counts.sort_values("count", ascending=False).head(10)
    
    fig = px.bar(
        product_counts,
        y="title",
        x="count",
        title="Top 10 Products by Review Count",
        orientation='h',
        color_discrete_sequence=['#1f77b4']
    )
    
    fig.update_layout(
        xaxis_title="Review Count",
        yaxis_title="Product Title",
        yaxis=dict(autorange="reversed")
    )
    
    return fig

# Callback for the brand mentions chart
@callback(
    Output('brand-mentions-chart', 'figure'),
    Input('year-slider', 'value')
)
def update_brand_mentions(years_range):
    filtered_df = detailed_df[
        (detailed_df["year"] >= years_range[0]) & 
        (detailed_df["year"] <= years_range[1])
    ]
    
    filtered_df["year_month"] = pd.to_datetime(filtered_df[["year", "month"]].assign(day=1))
    mentions_by_month = filtered_df.groupby(pd.Grouper(key="year_month", freq="ME"))[
        ["mentions_xbox", "mentions_nintendo", "mentions_sony"]
    ].sum().reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=mentions_by_month["year_month"],
        y=mentions_by_month["mentions_xbox"],
        mode='lines+markers',
        name='Xbox',
        line=dict(color='green')
    ))
    
    fig.add_trace(go.Scatter(
        x=mentions_by_month["year_month"],
        y=mentions_by_month["mentions_nintendo"],
        mode='lines+markers',
        name='Nintendo',
        line=dict(color='red')
    ))
    
    fig.add_trace(go.Scatter(
        x=mentions_by_month["year_month"],
        y=mentions_by_month["mentions_sony"],
        mode='lines+markers',
        name='Sony',
        line=dict(color='blue')
    ))
    
    fig.update_layout(
        title="Brand Mentions Over Time",
        xaxis_title="Date",
        yaxis_title="Number of Mentions",
        legend_title="Brand"
    )
    
    return fig

# Callback for the controller sentiment chart
@callback(
    Output('controller-sentiment-chart', 'figure'),
    Input('year-slider', 'value')
)
def update_controller_sentiment(years_range):
    filtered_df = detailed_df[
        (detailed_df["year"] >= years_range[0]) & 
        (detailed_df["year"] <= years_range[1]) &
        (detailed_df["mentions_controller"] == True)
    ]
    
    filtered_df["year_month"] = pd.to_datetime(filtered_df[["year", "month"]].assign(day=1))
    sentiment_by_month = filtered_df.groupby([pd.Grouper(key="year_month", freq="ME"), "sentiment"]).size().reset_index(name="count")
    
    fig = px.line(
        sentiment_by_month,
        x="year_month",
        y="count",
        color="sentiment",
        title="Controller Mentions by Sentiment Over Time",
        markers=True
    )
    
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Reviews",
        legend_title="Sentiment"
    )
    
    return fig

# Callback for the review text wordcloud
@callback(
    Output('wordcloud-image', 'src'),
    [Input('rating-slider', 'value')]
)
def update_wordcloud(rating_range):
    min_rating, max_rating = rating_range
    review_text = get_filtered_review_text(detailed_df, min_rating, max_rating, "review_text")
    
    img_str = create_wordcloud(review_text)
    return f'data:image/png;base64,{img_str}'

# Callback for the review title wordcloud
@callback(
    Output('wordcloud-title-image', 'src'),
    [Input('rating-slider', 'value')]
)
def update_title_wordcloud(rating_range):
    min_rating, max_rating = rating_range
    title_text = get_filtered_review_text(detailed_df, min_rating, max_rating, "title_text")
    
    img_str = create_wordcloud(title_text)
    return f'data:image/png;base64,{img_str}'

# Run the app
if __name__ == '__main__':
    app.run(debug=True)