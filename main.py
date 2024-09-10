import config
import json
import time
import os
# from flask import Flask, jsonify, Response
# from blueprints.activities import activities
# from microservices.create_data_table import spanner_app
# from microservices.read_services import read_data_from_spanner

# Cloud Spanner configurations
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc2"
DATABASE_ID = "rthe-poc"


# def create_app():
#     app = Flask(__name__)
#     app.register_blueprint(activities, url_prefix="/api/v1/activities")
#
#     # Error 404 handler
#     @app.errorhandler(404)
#     def resource_not_found(e):
#         return jsonify(error=str(e)), 404
#
#     # Error 405 handler
#     @app.errorhandler(405)
#     def resource_not_found(e):
#         return jsonify(error=str(e)), 405
#
#     # Error 401 handler
#     @app.errorhandler(401)
#     def custom_401(error):
#         return Response("API Key required.", 401)
#
#     @app.route("/ping")
#     def hello_world():
#         return "pong"
#
#     @app.route("/version", methods=["GET"], strict_slashes=False)
#     def version():
#         response_body = {
#             "success": 1,
#         }
#         return jsonify(response_body)
#
#     @app.after_request
#     def after_request(response):
#         if response and response.get_json():
#             data = response.get_json()
#
#             data["time_request"] = int(time.time())
#             data["version"] = config.VERSION
#
#             response.set_data(json.dumps(data))
#
#         return response
#
#     return app

# app = create_app()
#
# def create_spanner_table():
#     spanner_app.create_table()
#
#
# def read_spanner_table():
#     read_data_from_spanner(INSTANCE_ID, DATABASE_ID, "sample_table")


# read = read_service()

if __name__ == "__main__":
    print("Starting creating spanner database table...")
    # app.run(host="0.0.0.0", port=5000)
    # create_spanner_table()
    # read_spanner_table()
