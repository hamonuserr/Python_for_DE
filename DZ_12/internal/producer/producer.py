import time
import json
import psycopg2
from kafka import KafkaProducer


KAFKA_BROKER = "kafka:9092"  
KAFKA_TOPIC = "transactions"

DB_CONFIG = {
    "host": "postgres",       
    "port": 5432,             
    "database": "db-name",    
    "user": "login",          
    "password": "pass"        
}

def wait_for_postgres():
    retries = 5
    while retries > 0:
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            print("Successfully connected to the database!")
            return connection
        except Exception as e:
            print(f"Database not ready, retrying... ({retries} attempts left)")
            time.sleep(5)
            retries -= 1
    raise Exception("Failed to connect to the database after multiple retries")


def fetch_transactions():
    try:
        connection = wait_for_postgres()
        cursor = connection.cursor()
        query = "SELECT transaction_id, user_id, amount, timestamp, location FROM transactions;"
        cursor.execute(query)
        transactions = cursor.fetchall()
        print(f"Fetched {len(transactions)} transactions.")
        return transactions
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()


def send_to_kafka(producer, transactions):
    for transaction in transactions:
        message = {
            "transaction_id": transaction[0],
            "user_id": transaction[1],
            "amount": float(transaction[2]),
            "timestamp": str(transaction[3]),
            "location": transaction[4]
        }

        # Send the message to Kafka
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Sent to Kafka: {message}")


def main():
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("Starting to send transactions to Kafka...")
    try:
        # Fetch transactions from PostgreSQL
        transactions = fetch_transactions()

        if transactions:
            # Send transactions to Kafka
            send_to_kafka(producer, transactions)
        else:
            print("No transactions found.")

    except KeyboardInterrupt:
        print("Process interrupted.")
    finally:
        producer.close()
        print("Kafka producer closed.")

if __name__ == "__main__":
    time.sleep(15)
    main()
