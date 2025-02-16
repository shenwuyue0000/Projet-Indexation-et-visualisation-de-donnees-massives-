import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as _sum, max as _max, min as _min, col, explode

# Elasticsearch Configuration
es_host = "http://localhost:9200"
es_index = "processed_movies_index"
query = {
    "query": {
        "match_all": {}
    }
}

# Fetch Data from Elasticsearch
response = requests.get(f"{es_host}/{es_index}/_search", json=query)
data = response.json()

# Parse Data
hits = data['hits']['hits']
records = [hit['_source'] for hit in hits]

# Initialize Spark Session
spark = SparkSession.builder.appName("ElasticsearchDataProcessing").getOrCreate()
df = spark.createDataFrame(pd.DataFrame(records))

# Data Loading
df.show(5)  # Displaying first 5 records

# Data Processing
# Global statistical indicators
df.select(
    avg("vote_average").alias("avg_rating"),
    _sum("vote_count").alias("total_votes"),
    _max("vote_average").alias("max_rating"),
    _min("vote_average").alias("min_rating")
).show()

# Handling missing values (filling with default values)
df_clean = df.fillna({"vote_average": 0, "vote_count": 0})

# Exploding genre_ids to create individual rows for each genre_id
df_exploded = df_clean.withColumn("genre_id", explode("genre_ids"))

# Grouping and Aggregation by genre_id
df_grouped = df_exploded.groupBy("genre_id").agg(
    avg("vote_average").alias("avg_genre_rating"),
    _sum("vote_count").alias("total_genre_votes")
)
df_grouped.show()

# Data Transformation using map, filter, reduceByKey
rdd = df.rdd

# Example: Filtering movies with rating > 7
filtered_rdd = rdd.filter(lambda row: row['vote_average'] > 7)

# Mapping to key-value pairs (genre_id, vote_count)
mapped_rdd = filtered_rdd.flatMap(lambda row: [(genre_id, row['vote_count']) for genre_id in row['genre_ids']] if row['genre_ids'] else [])

# Reducing by key to sum votes per genre
reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

# Displaying the transformed data
for record in reduced_rdd.collect():
    print(record)

# Stop Spark Session
spark.stop()
