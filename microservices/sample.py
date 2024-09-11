from flask import Flask, jsonify
from google.cloud import spanner
import os

spanner_app = Flask(__name__)

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc2"


# Function to create the person_patient table
def create_person_patient_table():
    from google.cloud.spanner_admin_database_v1.types import \
        spanner_database_admin
    try:
        spanner_client = spanner.Client()
        instance = spanner_client.instance(INSTANCE_ID)
        database = instance.database(DATABASE_ID)
        print("person_patient table start.")
        # DDL statement to create the person_patient table
        ddl = """
        CREATE TABLE person_patient (
            person_id STRING(36) NOT NULL,
            first_name STRING(100),
            last_name STRING(100),
            gender STRING(10),
            party_id STRING(36),
            PRIMARY KEY (person_id)
        )
        """

        # Update the schema to create the table
        operation = database.update_ddl([ddl])
        operation.result()  # Wait for the operation to complete
        print("person_patient table created.")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise


@spanner_app.route('/create-table', methods=['POST'])
def create_table():
    try:
        # create_person_patient_table()
        create_database(INSTANCE_ID, DATABASE_ID)
        return jsonify({"message": "Table person_patient created successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def create_database(instance_id, database_id):
    """Creates a database and tables for sample data."""
    from google.cloud.spanner_admin_database_v1.types import \
        spanner_database_admin

    spanner_client = spanner.Client()
    database_admin_api = spanner_client.database_admin_api

    request = spanner_database_admin.CreateDatabaseRequest(
        parent=database_admin_api.instance_path(spanner_client.project, instance_id),
        create_statement=f"CREATE DATABASE `{database_id}`",
        extra_statements=[
            """CREATE TABLE Encounter_michelle (
            EncounterId     INT64 NOT NULL,
            FirstName    STRING(1024),
            LastName     STRING(1024),
            PersonInfo   BYTES(MAX),
            FullName   STRING(2048) AS (
                ARRAY_TO_STRING([FirstName, LastName], " ")
            ) STORED
        ) PRIMARY KEY (EncounterId)""",
            """CREATE TABLE Person (
            PartyId     INT64 NOT NULL,
            EncounterId     INT64 NOT NULL,
            PersonId      INT64 NOT NULL
        ) PRIMARY KEY (EncounterId, PartyId),
        INTERLEAVE IN PARENT Encounter_michelle ON DELETE CASCADE""",
        ],
    )

    operation = database_admin_api.create_database(request=request)

    print("Waiting for operation to complete...")
    database = operation.result(60)

    print(
        "Created database {} on instance {}".format(
            database.name,
            database_admin_api.instance_path(spanner_client.project, instance_id),
        )
    )


if __name__ == '__main__':
    spanner_app.run(debug=True)
    # Ensure the GOOGLE_APPLICATION_CREDENTIALS environment variable is set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("ERROR: The GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)