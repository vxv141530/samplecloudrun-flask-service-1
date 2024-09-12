from microservices.pubsub_to_spanner import PubSubToSpannerService
from microservices.spanner_to_pubsub import SpannerToPubSubService

# Configuration values
PROJECT_ID = "asc-ahnat-rthe-sandbox-poc"
INSTANCE_ID = "the-poc1"
DATABASE_ID = "rthe-poc1"
TOPIC_OUTBOUND_ID = "poc-topic-outbound"
TOPIC_INBOUND_SUB = "poc-topic-inbound-sub"
TABLE_NAME = "Encounter"

if __name__ == "__main__":
    # Instantiate the class with configuration for inbound pubsub messages
    listener = PubSubToSpannerService(
        project_id=PROJECT_ID,
        subscription_id=TOPIC_INBOUND_SUB,
        instance_id=INSTANCE_ID,
        database_id=DATABASE_ID,
        table_name=TABLE_NAME,
        timeout=3600.0
    )

    # Start listening for Pub/Sub messages
    listener.listen_for_messages()

    # Create an instance of the SpannerPubSubService
    service = SpannerToPubSubService(PROJECT_ID, INSTANCE_ID, DATABASE_ID, TOPIC_OUTBOUND_ID, TABLE_NAME)

    # Execute the service
    service.execute()
