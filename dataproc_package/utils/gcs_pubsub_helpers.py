import json


def publish_to_topic_with_dictionary_payload(
    project_id: str, topic_name: str, payload: dict, publisher
):
    """
    Publishes a message to a Pub/Sub topic with the given payload.

    """
    try:
        payload_json = json.dumps(payload)
        payload_encoded = payload_json.encode("utf-8")
        topic_path = publisher.topic_path(project_id, topic_name)
        future = publisher.publish(topic_path, data=payload_encoded)
        future.result()

        print(f"Published message to {topic_path} with payload: {payload}")
    except Exception as e:
        print(f"Error publishing message to {topic_path} with payload: {payload}")
        raise e
