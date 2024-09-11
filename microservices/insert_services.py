from google.cloud import spanner
import base64

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc2"

# Create a Spanner client
spanner_client = spanner.Client(project=PROJECT_ID)

# Get the instance and database
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)


def insert_encounter(encounter_id, person_id, first_name, last_name, person_info):
    # Encoding PersonInfo to bytes
    person_info_bytes = base64.b64encode(person_info.encode('utf-8'))

    with database.batch() as batch:
        batch.insert(
            table='Encounter',
            columns=('EncounterId', 'PersonId', 'FirstName', 'LastName', 'PersonInfo'),
            values=[
                (encounter_id, person_id, first_name, last_name, person_info_bytes),
            ]
        )
    print(f"Inserted encounter for: {first_name} {last_name}")


if __name__ == "__main__":
    # Cloud Spanner configurations
    INSTANCE_ID = "the-poc1"
    DATABASE_ID = "rthe-poc1"
    TABLE_NAME = "Encounter"
    # Insert a record into the 'Encounter' table
    insert_encounter(345, 100, 'Charles', 'Doe', 'Person Info: Details about Charles Doe')
