from kafka import KafkaProducer,KafkaConsumer
import json

from app.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    QR_SCAN_TOPIC,
    VALIDATION_RESULTS_TOPIC,
    RETRY_VALIDATION_TOPIC,
    DEAD_LETTER_TOPIC
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

validation_consumer = KafkaConsumer(
    QR_SCAN_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    group_id="validation-group",
    enable_auto_commit=False,
    value_deserializer = lambda v: json.loads(v.decode("utf-8"))
)

validation_results_consumer = KafkaConsumer(
    VALIDATION_RESULTS_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    group_id="gateway-group",
    enable_auto_commit=False,
    value_deserializer = lambda v: json.loads(v.decode("utf-8"))
)

retry_consumer = KafkaConsumer(
    RETRY_VALIDATION_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    group_id="retry-group",
    enable_auto_commit=False,
    value_deserializer = lambda v: json.loads(v.decode("utf-8"))
)

dead_letter_consumer = KafkaConsumer(
    DEAD_LETTER_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    group_id="dead-letter-group",
    enable_auto_commit=False,
    value_deserializer = lambda v: json.loads(v.decode("utf-8"))
)

analytics_consumer = KafkaConsumer(
    VALIDATION_RESULTS_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="analytics-group",
    enable_auto_commit=False,
    value_deserializer = lambda v: json.loads(v.decode("utf-8"))
)

audit_consumer = KafkaConsumer(
    VALIDATION_RESULTS_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="audit-group",
    enable_auto_commit=False,
    value_deserializer = lambda v: json.loads(v.decode("utf-8"))
)