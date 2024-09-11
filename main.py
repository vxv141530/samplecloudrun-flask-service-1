import os

import config
import json
import time
from flask import jsonify, Response
from blueprints.activities import activities
from microservices.read_services import read_data_from_spanner
from flask import Flask, request

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc2"
DATABASE_ID = "rthe-poc"


def create_services():
    services = Flask(__name__)
    services.register_blueprint(activities, url_prefix="/api/v1/activities")

    # Error 404 handler
    @services.errorhandler(404)
    def resource_not_found(e):
        return jsonify(error=str(e)), 404

    # Error 405 handler
    @services.errorhandler(405)
    def resource_not_found(e):
        return jsonify(error=str(e)), 405

    # Error 401 handler
    @services.errorhandler(401)
    def custom_401(error):
        return Response("API Key required.", 401)

    @services.route("/ping")
    def hello_world():
        return "pong"

    @services.route("/read_service/query", methods=["GET"], strict_slashes=False)
    def read_service_query():
        instance_id = request.args.get("instance_id")
        database_id = request.args.get("database_id")
        table_name = request.args.get("table_name")

        results = read_data_from_spanner(instance_id, database_id, table_name)
        response_body = {
            "success": 1,
            "data": results
        }
        return jsonify(response_body)

    @services.route("/version", methods=["GET"], strict_slashes=False)
    def version():
        response_body = {
            "success": 1,
        }
        return jsonify(response_body)

    @services.after_request
    def after_request(response):
        if response and response.get_json():
            data = response.get_json()
            data["time_request"] = int(time.time())
            data["version"] = config.VERSION

            response.set_data(json.dumps(data))

        return response

    return services


services = create_services()

if __name__ == "__main__":
    print("Starting creating spanner database table...")
    services.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
