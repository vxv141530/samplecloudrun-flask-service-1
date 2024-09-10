# from google.cloud import spanner
#
#
# def read_data_from_spanner(instance_id, database_id, table_name):
#     spanner_client = spanner.Client()
#
#     # Connect to the Spanner instance and database
#     instance = spanner_client.instance(instance_id)
#     database = instance.database(database_id)
#
#     # Create a query to fetch data from the specified table
#     query = f"SELECT * FROM {table_name}"
#
#     # Execute the query
#     with database.snapshot() as snapshot:
#         results = snapshot.execute_sql(query)
#
#         # Iterate over and print the results
#         for row in results:
#             print(row)
#
#
# if __name__ == "__main__":
#     # Cloud Spanner configurations
#     INSTANCE_ID = "the-poc1"
#     DATABASE_ID = "rthe-poc"
#     TABLE_NAME = "test_table"
#
#     read_data_from_spanner(INSTANCE_ID, DATABASE_ID, TABLE_NAME)
