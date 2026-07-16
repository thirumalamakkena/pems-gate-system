import threading
import time

from app.metrics import metrics_buffer
from app.repositories.metrics_repository import MetricsRepository


class MetricsFlusher:

    def __init__(self):

        self.repository = MetricsRepository()

    def start(self):

        thread = threading.Thread(
            target=self.run,
            daemon=True
        )

        thread.start()

    def run(self):

        while True:

            snapshot = metrics_buffer.snapshot()

            if snapshot:

                for service_name, metrics in snapshot.items():

                    self.repository.update_metrics(
                        service_name,
                        metrics
                    )

            time.sleep(5)