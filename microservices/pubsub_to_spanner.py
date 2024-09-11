import os
import json
from google.cloud import pubsub_v1
from google.cloud import spanner
from concurrent.futures import TimeoutError

# Cloud Pub/Sub configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
SUBSCRIPTION_ID = "poc-topic1-sub"
TIMEOUT = 60.0  # Time to listen for messages (in seconds)

# Cloud Spanner configurations
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc1"
TABLE_NAME = "Encounter"

# Initialize Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# Initialize Spanner client
spanner_client = spanner.Client(project=PROJECT_ID)
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)


# Function to insert a record into the Encounter table in Cloud Spanner
def insert_into_spanner(encounter_id, person_id, first_name, last_name):
    with database.batch() as batch:
        batch.insert(
            table="Encounter",
            columns=("EncounterId","PersonId", "FirstName", "LastName"),
            values=[(encounter_id, person_id, first_name, last_name)]
        )
    print(f"Record inserted into Encounter table with EncounterId: {encounter_id}")


# Callback function to process Pub/Sub messages
def callback(message):
    print(f"Received message: {message.data.decode('utf-8')}")
    message_data = json.loads(message.data.decode('utf-8'))

    # Extract the necessary fields from the message (assuming the Pub/Sub message contains these fields)
    encounter_id = message_data.get('EncounterId')
    person_id = message_data.get('PersonId')
    first_name = message_data.get('FirstName')
    last_name = message_data.get('LastName')
    person_info = message_data.get('PersonInfo')  # Assuming this is in a byte-encoded format

    # Insert the data into the Cloud Spanner Encounter table
    insert_into_spanner(encounter_id, person_id, first_name, last_name)

    # Acknowledge the message
    message.ack()


# Start listening to the Pub/Sub topic
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the main thread alive while listening for messages
try:
    streaming_pull_future.result(timeout=TIMEOUT)
except TimeoutError:
    streaming_pull_future.cancel()  # Stop the subscriber if timeout occurs
    streaming_pull_future.result()

print("Stopped listening for messages.")
