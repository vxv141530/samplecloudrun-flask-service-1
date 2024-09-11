from google.cloud import spanner
from google.cloud.spanner_admin_database_v1.types import \
    spanner_database_admin

PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc2"


def create_table():
    spanner_client = spanner.Client(project=PROJECT_ID)

    instance = spanner_client.instance(INSTANCE_ID)
    database_admin_api = spanner_client.database_admin_api

    request = spanner_database_admin.CreateDatabaseRequest(
        parent=database_admin_api.instance_path(spanner_client.project, INSTANCE_ID),
        create_statement=f"CREATE DATABASE `{DATABASE_ID}`",
        extra_statements=[
            """CREATE TABLE Encounter (
            EncounterId     INT64 NOT NULL,
            PersonId INT64 NOT NULL, 
            FirstName    STRING(1024),
            LastName     STRING(1024),
            PersonInfo   BYTES(MAX),
            FullName   STRING(2048) AS (
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
