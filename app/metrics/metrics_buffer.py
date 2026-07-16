from threading import Lock

class MetricsBuffer:

    def __init__(self):

        self.lock = Lock()

        self.metrics = {}

    def increment(
        self,
        service_name,
        metric_name
    ):

        with self.lock:

            self._initialize(service_name)

            if metric_name not in self.metrics[service_name]:
                self.metrics[service_name][metric_name] = 0

            self.metrics[service_name][metric_name] += 1

    

    def add(
        self,
        service_name,
        metric_name,
        value
    ):

        with self.lock:

            self._initialize(service_name)

            if metric_name not in self.metrics[service_name]:
                self.metrics[service_name][metric_name] = 0

            self.metrics[service_name][metric_name] += value

    def snapshot(self):

        with self.lock:

            snapshot = self.metrics

            self.metrics = {}

            return snapshot

    def _initialize(
        self,
        service_name
    ):

        if service_name not in self.metrics:

            self.metrics[service_name] = {}
    