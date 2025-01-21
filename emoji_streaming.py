from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from confluent_kafka import Producer
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .getOrCreate()

# Suppress verbose logs
spark.sparkContext.setLogLevel("ERROR")

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9092"  # Broker from which we read stream data
kafka_topic = "emoji_stream"
kafka_publish_topic = "aggregated_emojis"  # Producer sends aggregated data here

# Initialize Kafka producer with configuration dictionary
producer_config = {
    'bootstrap.servers': "localhost:9092"  # Ensure producer sends data to this broker
}
producer = Producer(producer_config)

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Updated streaming DataFrame without aggregation
emoji_df = df.selectExpr("CAST(value AS STRING)") \
    .selectExpr("json_tuple(value, 'user_id', 'emoji_type') as (user_id, emoji_type)")

# Function to process each batch of emoji counts and send results to Kafka
def process_batch(batch_df, batch_id):
    print(f"\nProcessing batch {batch_id}\n")

    # Group and count emoji types only for the current batch
    batch_emoji_counts = batch_df.groupBy("emoji_type").count().withColumnRenamed("count", "batch_count").collect()

    if batch_emoji_counts:
        # Set an aggregation threshold for scaling down high counts
        aggregation_threshold = 700
        aggregated_emoji_counts = []

        # Apply scaling logic or set to 0 if below threshold
        for row in batch_emoji_counts:
            emoji_type = row['emoji_type']
            batch_count = row['batch_count']
            if batch_count > aggregation_threshold:
                count_scaled = batch_count // aggregation_threshold
                aggregated_emoji_counts.extend([emoji_type] * count_scaled)

        # Print current batch counts with aggregation applied
        print("Emoji | Batch Count | Aggregated List")
        print("-" * 40)
        for row in batch_emoji_counts:
            emoji = row['emoji_type']
            batch_count = row['batch_count']
            aggregated_count = aggregated_emoji_counts.count(emoji)
            print(f"{emoji:<3} | {batch_count:<11} | {' '.join([emoji] * aggregated_count)}")

        # Print the aggregated emojis directly
        print("\nAggregated Emojis:")
        print(aggregated_emoji_counts)

        # Send aggregated list to Kafka asynchronously
        if aggregated_emoji_counts:
            #message = {"batch_id": batch_id, "aggregated_emojis": aggregated_emoji_counts}
            message = aggregated_emoji_counts
            producer.produce(kafka_publish_topic, value=json.dumps(message).encode('utf-8'))
            print("Aggregated list sent to Kafka:", message)
        else:
            print("No aggregated emojis to send.")

    else:
        print("No emojis in this batch.")

# Output to the console for each batch without accumulating previous results
query = emoji_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="2 seconds") \
    .start()

# Await termination
query.awaitTermination()
