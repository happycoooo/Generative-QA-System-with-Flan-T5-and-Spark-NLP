import time
import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka Producer for QA Data") \
    .getOrCreate()

# Kafka broker
broker = 'localhost:9092'

# Initialize Kafka producer with JSON serializer
producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# Read the Parquet file
file_path = "/data/lab/project2/squad_v2/squad_v2/validation-00000-of-00001.parquet"

df = spark.read.parquet(file_path)

print(">>> Now successfully start the kafka producer process")
print("Now running in background. Do not exit...")

# Send each row of the DataFrame as a JSON message to the Kafka topic 'q3'
for row in df.collect():
    message = {
        'id': row['id'],
        'title': row['title'],
        'context': row['context'],
        'question': row['question'],
        'answers': {
            'text': row['answers']['text'],
            'answer_start': row['answers']['answer_start']
        },
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # Current system time
    }
    # Send the message to Kafka
    producer.send('q3', message)
    print(message)
    time.sleep(0.5)  # Delay to simulate real-time data feeding

# Ensure all messages are sent before closing
producer.flush()
print(">>> Finished sending messages.")
