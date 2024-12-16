from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, hour
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from kafka import KafkaProducer
import psycopg2
import time

# Kafka configuration
KAFKA_BROKER = "kafka:9092"
TRANSACTIONS_TOPIC = "transactions"
SUSPICIOUS_TOPIC = "suspicious"

# PostgreSQL configuration
DB_CONFIG = {
    "host": "postgres",  
    "port": 5432,
    "database": "db-name",
    "user": "login",
    "password": "pass"
}

# Thresholds
TRANSACTION_THRESHOLD = 1000  
SUSPICIOUS_HOURS = (23, 5)  

def main():
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Fetch user data from PostgreSQL
    def fetch_user_data():
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute("SELECT user_id, registration_address, last_known_location FROM client_info;")
            data = cursor.fetchall()
            return {row[0]: {"registration_address": row[1], "last_known_location": row[2]} for row in data}
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()


    user_data = fetch_user_data()

    # Define Spark session
    spark = SparkSession.builder \
        .appName("Jupyter PySpark Test") \
        .getOrCreate()


    # Kafka transaction schema
    transaction_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", FloatType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StringType(), True)
    ])

    # Read streaming data from Kafka
    transactions_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TRANSACTIONS_TOPIC) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # Parse JSON data
    transactions_parsed = transactions_df.select(
        from_json(col("value"), transaction_schema).alias("data")
    ).select("data.*")

    # Convert timestamp column to proper type
    transactions_parsed = transactions_parsed.withColumn(
        "timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")
    )

    # Add hour column to filter suspicious times
    transactions_with_hour = transactions_parsed.withColumn("hour", hour("timestamp"))

    # Define the suspicious conditions
    def is_suspicious(transaction):
        user_info = user_data.get(transaction.user_id, {})
        registration_address = user_info.get("registration_address", "")
        last_known_location = user_info.get("last_known_location", "")

        return (
            transaction.amount > TRANSACTION_THRESHOLD or
            (transaction.hour >= SUSPICIOUS_HOURS[0] or transaction.hour < SUSPICIOUS_HOURS[1]) or
            (transaction.location != registration_address and transaction.location != last_known_location)
        )

    # Filter suspicious transactions
    def process_and_send_suspicious(batch_df, batch_id):
        suspicious_transactions = batch_df.rdd.filter(is_suspicious).collect()
        for transaction in suspicious_transactions:
            message = {
                "transaction_id": transaction.transaction_id,
                "user_id": transaction.user_id,
                "amount": transaction.amount,
                "timestamp": str(transaction.timestamp),
                "location": transaction.location
            }
            producer.send(SUSPICIOUS_TOPIC, value=message)
            print(f"Suspicious Transaction: {message}")

    # Start streaming query
    query = transactions_with_hour.writeStream \
        .outputMode("append") \
        .foreachBatch(process_and_send_suspicious) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    time.sleep(15)  
    main()          