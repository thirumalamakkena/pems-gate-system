from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    ("qr-scans", 3),
    ("validation-results", 3),
    ("retry-validation", 3),
    ("dead-letter-events", 3),
]


def create_topics():

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="pems-topic-initializer"
    )

    existing_topics = admin.list_topics()

    topics_to_create = []

    for topic_name, partitions in TOPICS:

        if topic_name not in existing_topics:

            topics_to_create.append(
                NewTopic(
                    name=topic_name,
                    num_partitions=partitions,
                    replication_factor=1
                )
            )

    if topics_to_create:

        admin.create_topics(
            new_topics=topics_to_create,
            validate_only=False
        )

        print("Topics created:")

        for topic in topics_to_create:
            print(f"  ✓ {topic.name}")

    else:
        print("All topics already exist.")

    

    admin.close()


if __name__ == "__main__":

    try:
        create_topics()

    except TopicAlreadyExistsError:
        print("Topics already exist.")

    except Exception as e:
        print(f"Error: {e}")
