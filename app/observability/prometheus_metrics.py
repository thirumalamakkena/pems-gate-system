from prometheus_client import (
    Counter,
    Gauge,
    Histogram
)

# ==========================================================
# PROCESSING METRICS
# ==========================================================

events_processed = Counter(
    "events_processed_total",
    "Total successfully processed events",
    ["service"]
)

events_failed = Counter(
    "events_failed_total",
    "Total failed events",
    ["service"]
)

processing_latency = Histogram(
    "event_processing_latency_ms",
    "Event processing latency in milliseconds",
    ["service"],
    buckets=(
        0.5,
        1,
        2,
        5,
        10,
        20,
        50,
        100,
        250,
        500,
        1000
    )
)

# ==========================================================
# CACHE METRICS
# ==========================================================

cache_hits = Counter(
    "cache_hits_total",
    "Total cache hits",
    ["service"]
)

cache_misses = Counter(
    "cache_misses_total",
    "Total cache misses",
    ["service"]
)

# ==========================================================
# RELIABILITY METRICS
# ==========================================================

retry_events = Counter(
    "retry_events_total",
    "Total events enqueued to retry topic",
    ["service"]
)

retry_success = Counter(
    "retry_success_total",
    "Total events successfully processed after retry",
    ["service"]
)

dlq_events = Counter(
    "dlq_events_total",
    "Total events moved to dead letter queue",
    ["service"]
)

dlq_replay_success = Counter(
    "dlq_replay_success_total",
    "Total events successfully replayed from DLQ",
    ["service"]
)

# ==========================================================
# KAFKA METRICS
# ==========================================================

kafka_messages_produced = Counter(
    "kafka_messages_produced_total",
    "Total Kafka messages produced",
    ["service", "topic"]
)

kafka_messages_consumed = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["service", "topic"]
)

# ==========================================================
# DATABASE METRICS
# ==========================================================

mongodb_writes = Counter(
    "mongodb_writes_total",
    "Total MongoDB write operations",
    ["service", "collection"]
)

# ==========================================================
# BUSINESS METRICS
# ==========================================================

validation_results = Counter(
    "validation_results_total",
    "Validation decision counts",
    ["status"]
)

# Status Values
#
# VALID
# INVALID
# UNKNOWN_USER
# UNKNOWN_GATEWAY
# EXPIRED

# ==========================================================
# GATEWAY METRICS
# ==========================================================

websocket_messages_sent = Counter(
    "websocket_messages_sent_total",
    "Total WebSocket responses sent"
)

# ==========================================================
# SERVICE HEALTH
# ==========================================================

active_consumers = Gauge(
    "active_consumers",
    "Current active consumer instances",
    ["service"]
)


# ==========================================================
# METRICS WRAPPER
# ==========================================================

class PrometheusMetrics:

    # ======================================================
    # PROCESSING
    # ======================================================

    def record_processed_event(
        self,
        service
    ):
        events_processed.labels(
            service=service
        ).inc()

    def record_failed_event(
        self,
        service
    ):
        events_failed.labels(
            service=service
        ).inc()

    def record_processing_latency(
        self,
        service,
        latency_ms
    ):
        processing_latency.labels(
            service=service
        ).observe(latency_ms)

    # ======================================================
    # CACHE
    # ======================================================

    def record_cache_hit(
        self,
        service
    ):
        cache_hits.labels(
            service=service
        ).inc()

    def record_cache_miss(
        self,
        service
    ):
        cache_misses.labels(
            service=service
        ).inc()

    # ======================================================
    # RELIABILITY
    # ======================================================

    def record_retry_enqueued(
        self,
        service
    ):
        retry_events.labels(
            service=service
        ).inc()

    def record_retry_success(
        self,
        service
    ):
        retry_success.labels(
            service=service
        ).inc()

    def record_dlq_event(
        self,
        service
    ):
        dlq_events.labels(
            service=service
        ).inc()

    def record_dlq_replay_success(
        self,
        service
    ):
        dlq_replay_success.labels(
            service=service
        ).inc()

    # ======================================================
    # KAFKA
    # ======================================================

    def record_kafka_produced(
        self,
        service,
        topic
    ):
        kafka_messages_produced.labels(
            service=service,
            topic=topic
        ).inc()

    def record_kafka_consumed(
        self,
        service,
        topic
    ):
        kafka_messages_consumed.labels(
            service=service,
            topic=topic
        ).inc()

    # ======================================================
    # DATABASE
    # ======================================================

    def record_mongodb_write(
        self,
        service,
        collection
    ):
        mongodb_writes.labels(
            service=service,
            collection=collection
        ).inc()

    # ======================================================
    # BUSINESS
    # ======================================================

    def record_validation_result(
        self,
        status
    ):
        validation_results.labels(
            status=status
        ).inc()

    # ======================================================
    # GATEWAY
    # ======================================================

    def record_websocket_message(self):
        websocket_messages_sent.inc()

    # ======================================================
    # HEALTH
    # ======================================================

    def consumer_started(
        self,
        service
    ):
        active_consumers.labels(
            service=service
        ).set(1)

    def consumer_stopped(
        self,
        service
    ):
        active_consumers.labels(
            service=service
        ).set(0)


prometheus_metrics = PrometheusMetrics()