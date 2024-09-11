from google.cloud import spanner
from google.cloud.spanner_admin_database_v1.types import \
    spanner_database_admin

PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc1"


def create_database_table():
    spanner_client = spanner.Client(project=PROJECT_ID)

    instance = spanner_client.instance(INSTANCE_ID)
    database_admin_api = spanner_client.database_admin_api

    request = spanner_database_admin.CreateDatabaseRequest(
        parent=database_admin_api.instance_path(spanner_client.project, INSTANCE_ID),
        create_statement=f"CREATE DATABASE `{DATABASE_ID}`",
        extra_statements=[
            """CREATE TABLE Encounter (
            EncounterId  INT64 NOT NULL,
            PersonId INT64 NOT NULL, 
            FirstName  STRING(1024),
            LastName  STRING(1024),
            PersonInfo  BYTES(MAX),
            FullName  STRING(2048) AS (
                ARRAY_TO_STRING([FirstName, LastName], " ")
            ) STORED
        ) PRIMARY KEY (EncounterId)""",
            """CREATE TABLE Coverage (
            EncounterId INT64 NOT NULL,
            PersonId INT64 NOT NULL, 
            PartyId INT64 NOT NULL,
            CONSTRAINT FK_encounter FOREIGN KEY (EncounterId) REFERENCES Encounter(EncounterId)
        ) PRIMARY KEY (EncounterId) """
        ],
    )
    operation = database_admin_api.create_database(request=request)

    print("Waiting for operation to complete...")
    database = operation.result()
    print(database)


def create_table(instance_id, database_id):
    # Create a Spanner client
    spanner_client = spanner.Client()

    # Create an instance object
    instance = spanner_client.instance(INSTANCE_ID)

    # Create a database object
    database = instance.database(DATABASE_ID)

    # Define the DDL statement to create the table
    ddl_statement = """
    CREATE TABLE Person (
        UserId INT64 NOT NULL,
        FirstName STRING(1024),
        LastName STRING(1024),
        Email STRING(1024),
        Age INT64,
        PRIMARY KEY (PersonId)
    )
    """

    # Execute the DDL statement to create the table
    operation = database.update_ddl([ddl_statement])

    # Wait for the operation to complete
    print("Waiting for operation to complete...")
    operation.result()

    print("Table created successfully.")


if __name__ == "__main__":
    # Cloud Spanner configurations
    INSTANCE_ID = "the-poc1"
    DATABASE_ID = "rthe-poc1"
    # TABLE_NAME = "Encounter"
    # create_database_table()
    create_table(INSTANCE_ID, DATABASE_ID)  # person table
    # read_data_from_spanner(INSTANCE_ID, DATABASE_ID, TABLE_NAME)
