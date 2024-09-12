import os
import json
from google.cloud import pubsub_v1
from google.cloud import spanner
from concurrent.futures import TimeoutError


class PubSubToSpannerService:
    def __init__(self, project_id, subscription_id, instance_id, database_id, table_name, timeout=60.0):
        # Cloud Pub/Sub configurations
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.timeout = timeout

        # Cloud Spanner configurations
        self.instance_id = instance_id
        self.database_id = database_id
        self.table_name = table_name

        # Initialize Pub/Sub subscriber client
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)

        # Initialize Spanner client
        self.spanner_client = spanner.Client(project=self.project_id)
        self.instance = self.spanner_client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)

    def insert_into_spanner(self, encounter_id, person_id, first_name, last_name):
        """Inserts a record into the Encounter table in Cloud Spanner."""
        with self.database.batch() as batch:
            batch.insert(
                table=self.table_name,
                columns=("EncounterId", "PersonId", "FirstName", "LastName"),
                values=[(encounter_id, person_id, first_name, last_name)]
            )
        print(f"Record inserted into {self.table_name} table with EncounterId: {encounter_id}")

    def callback(self, message):
        """Processes Pub/Sub messages and inserts data into Cloud Spanner."""
        print(f"Received message: {message.data.decode('utf-8')}")
        message_data = json.loads(message.data.decode('utf-8'))

        # Extract the necessary fields from the message (assuming the Pub/Sub message contains these fields)
        encounter_id = message_data.get('EncounterId')
        person_id = message_data.get('PersonId')
        first_name = message_data.get('FirstName')
        last_name = message_data.get('LastName')

        # Insert the data into the Cloud Spanner Encounter table
        self.insert_into_spanner(encounter_id, person_id, first_name, last_name)

        # Acknowledge the message
        message.ack()

    def listen_for_messages(self):
        """Listens to the Pub/Sub topic for messages."""
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_path}...")

        try:
            streaming_pull_future.result(timeout=self.timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Stop the subscriber if timeout occurs
            streaming_pull_future.result()
            print("Stopped listening for messages due to timeout.")


# if __name__ == "__main__":
    # # Instantiate the class with configuration parameters
    # listener = SpannerPubSubListener(
    #     project_id="asc-ahnat-rthe-sandbox-poc",
    #     subscription_id="poc-topic-inbound-sub",
    #     instance_id="the-poc1",
    #     database_id="rthe-poc1",
    #     table_name="Encounter",
    #     timeout=3600.0
    # )
    #
    # # Start listening for messages
    # listener.listen_for_messages()
