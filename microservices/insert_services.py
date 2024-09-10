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


# Function to insert data into the 'Encounter' table
def insert_encounter(encounter_id, first_name, last_name, person_info):
    # Encoding PersonInfo to bytes
    person_info_bytes = base64.b64encode(person_info.encode('utf-8'))

    with database.batch() as batch:
        batch.insert(
            table='Encounter',
            columns=('EncounterId', 'FirstName', 'LastName', 'PersonInfo'),
            values=[
                (encounter_id, first_name, last_name, person_info_bytes),
            ]
        )
    print(f"Inserted encounter for: {first_name} {last_name}")


# Insert a record into the 'Encounter' table
insert_encounter(1, 'John', 'Doe', 'Person Info: Details about John')
