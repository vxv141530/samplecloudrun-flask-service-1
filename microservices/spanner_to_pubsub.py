from google.cloud import spanner
from google.cloud import pubsub_v1
import json

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"

# Pub/Sub configurations
PUBSUB_TOPIC_ID = "poc-topic-outbound"

# Cloud Spanner configurations
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc1"
TABLE_NAME = "Encounter"

# Initialize Spanner client
spanner_client = spanner.Client(project=PROJECT_ID)
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)

# Initialize Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)


def read_encounter_data_from_spanner():
    """Read encounter data from Cloud Spanner"""
    with database.snapshot() as snapshot:
        query = "SELECT EncounterId, PersonId, FirstName, LastName, FullName FROM Encounter LIMIT 1;"
        results = snapshot.execute_sql(query)

        encounter_list = []
        for row in results:
            encounter_data = {
                "EncounterId": row[0],
                "PersonId": row[1],
                "FirstName": row[2],
                "LastName": row[3],
                "FullName": row[4]
            }
            encounter_list.append(encounter_data)

        return encounter_list


def publish_to_pubsub(encounters):
    """Publish encounter data to a Pub/Sub topic"""
    for encounter in encounters:
        message_json = json.dumps(encounter).encode("utf-8")
        future = publisher.publish(topic_path, message_json)
        print(f"Published message ID: {future.result()}")


def main():
    # Step 1: Read data from Cloud Spanner
    encounters = read_encounter_data_from_spanner()
    print(f"Retrieved {len(encounters)} records from Cloud Spanner.")

    # Step 2: Publish the data to a Pub/Sub topic
    publish_to_pubsub(encounters)
    print(f"Published {len(encounters)} messages to Pub/Sub.")


if __name__ == "__main__":
    main()
