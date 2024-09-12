from microservices.pubsub_to_spanner import SpannerPubSubListener


if __name__ == "__main__":
    # Instantiate the class with configuration
    listener = SpannerPubSubListener(
        project_id="asc-ahnat-rthe-sandbox-poc",
        subscription_id="poc-topic-inbound-sub",
        instance_id="the-poc1",
        database_id="rthe-poc1",
        table_name="Encounter",
        timeout=3600.0
    )

    # Start listening for Pub/Sub messages
    listener.listen_for_messages()