from flask import Flask, jsonify
from google.cloud import spanner
import os

spanner_app = Flask(__name__)

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc"


# Function to create the person_patient table
def create_person_patient_table():
    try:
        spanner_client = spanner.Client()
        instance = spanner_client.instance(INSTANCE_ID)
        database = instance.database(DATABASE_ID)

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

@app.route('/create-table', methods=['GET'])
def create_table():
    try:
        create_person_patient_table()
        return jsonify({"message": "Table person_patient created successfully!"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
    # Ensure the GOOGLE_APPLICATION_CREDENTIALS environment variable is set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("ERROR: The GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
        exit(1)
