import os
import config
import json
import time
from blueprints.activities import activities
from microservices.read_services import read_data_from_spanner
from microservices.delete_services import delete_data_from_spanner
from flask import Flask, request, jsonify, Response

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc2"
DATABASE_ID = "rthe-poc"

app = Flask(__name__)


def create_read_service():
    service_read = Flask(__name__)
    service_read.register_blueprint(activities, url_prefix="/api/v1/activities")

    # Error 404 handler
    @service_read.errorhandler(404)
    def resource_not_found(e):
        return jsonify(error=str(e)), 404

    @service_read.route("/read_service/hey", methods=["GET"], strict_slashes=False)
    def hello_world():
        """Example route."""
        name = os.environ.get("NAME", "World")
        return f"Hello {name}!"

    # Error 405 handler
    @service_read.errorhandler(405)
    def resource_not_found(e):
        return jsonify(error=str(e)), 405

    # Error 401 handler
    @service_read.errorhandler(401)
    def custom_401(error):
        return Response("API Key required.", 401)

    @service_read.route("/read_service/query", methods=["GET"], strict_slashes=False)
    def read_service_query():

        try:
            instance_id = request.args.get("instance_id")
            database_id = request.args.get("database_id")
            table_name = request.args.get("table_name")
            # Read data from Spanner
            results = read_data_from_spanner(instance_id, database_id, table_name)

            # Convert StreamedResultSet to a list of dictionaries or tuples
            rows = []
            for row in results:
                row_dict = {column_name: row[index] for index, column_name in enumerate(row.keys())}
                rows.append(row_dict)

            # Prepare response
            response_body = {
                "success": 1,
                "data": rows
            }

            return jsonify(response_body), 200

        except Exception as e:
            return jsonify({"success": 0, "error": str(e)}), 500

    @service_read.route("/delete_service/query", methods=["POST"], strict_slashes=False)
    def delete_service_query():

        try:
            instance_id = request.args.get("instance_id")
            database_id = request.args.get("database_id")
            table_name = request.args.get("table_name")
            primary_key_name = request.args.get("primary_key_name")
            primary_key_value = request.args.get("primary_key_value")
            # Read data from Spanner
            results = delete_data_from_spanner(instance_id, database_id, table_name, primary_key_name, primary_key_value)

            # Prepare response
            response_body = {
                "success": 1,
                "data": f"The {primary_key_name} with {primary_key_value} has been deleted from table {table_name} successfully."
            }

            return jsonify(response_body), 200

        except Exception as e:
            return jsonify({"success": 0, "error": str(e)}), 500

    @service_read.route("/version", methods=["GET"], strict_slashes=False)
    def version():
        response_value = {
            "success": 1,
        }
        return jsonify(response_value)

    @service_read.after_request
    def after_request(response):
        if response and response.get_json():
            data = response.get_json()
            data["time_request"] = int(time.time())
            data["version"] = config.VERSION

            response.set_data(json.dumps(data))

        return response

    return service_read


service_read = create_read_service()

if __name__ == "__main__":
    print("Starting reading spanner database tables...")
    # services = create_read_service()
    service_read.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
