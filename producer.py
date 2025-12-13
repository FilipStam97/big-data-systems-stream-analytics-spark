from kafka import KafkaProducer
import json
import time
import csv

BOOTSTRAP = "localhost:9092"
TOPIC_NAME = 'csv_data_topic'
CSV_FILE = 'yellow_tripdata_2016-01.csv'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


try: 
    with open(CSV_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader: 
            print(f"Sending row: {row}")
            producer.send(TOPIC_NAME, value=row)
             # Optional: producer.flush() here for immediate sending (less efficient)
            # producer.poll(0) # Poll for events if needed, as per {Link: kafka-python documentation https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html} [7, 8, 12]
            # time.sleep(0.1) # Small delay to avoid overwhelming Kafka
        
       #Flush remaining messages
    producer.flush()
    print("All messages sent and flushed.")

except FileNotFoundError:
    print(f"Error: {CSV_FILE} not found.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()