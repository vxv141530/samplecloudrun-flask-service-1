from google.cloud import spanner

PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"


def read_data_from_spanner(instance_id, database_id, table_name):
    spanner_client = spanner.Client(project=PROJECT_ID)

    # Connect to the Spanner instance and database
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Create a query to fetch data from the specified table
    query = f"SELECT * FROM {table_name}"

    print(query)
    # Execute the query
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(query)

    return results


if __name__ == "__main__":
    # Cloud Spanner configurations
    INSTANCE_ID = "the-poc1"
    DATABASE_ID = "rthe-poc2"
    TABLE_NAME = "Encounter"
    read_data_from_spanner(INSTANCE_ID, DATABASE_ID, TABLE_NAME)
