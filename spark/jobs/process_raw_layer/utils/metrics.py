from time import time, sleep
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway, delete_from_gateway
import threading

class Metrics:
    def __init__(self, schema, job, gateway):

        self.start_time = time()
        self.total_records = 0
        self.null_count = {}
        self.null_ratio = {}

        self.registry = CollectorRegistry()
        self.gateway = gateway
        self.job = job
        self.schema = schema

        # Metrics
        self.record_gauge = Gauge('total_records', 'Total records', ['job','schema'], registry=self.registry)
        self.null_gauge = Gauge('null_count', 'Null count per field', ['job','schema','field'], registry=self.registry)
        self.ratio_gauge = Gauge('null_ratio', 'Null ratio per field', ['job','schema','field'], registry=self.registry)
        self.processing_time = Gauge('batch_processing_time', 'Time taken to process a batch in seconds', ['job'], registry=self.registry)

    def reset(self):
        self.start_time = time()
        self.total_records = 0
        self.null_count = {}
        self.null_ratio = {}

    def update(self, obj):
        self.total_records += obj.get_count()
        for k,v in obj.get_null_count().items():
            self.null_count[k] = self.null_count.get(k, 0) + v

    def _null_ratio(self):
        for k,v in self.null_count.items():
            try:
                self.null_ratio[k] = float(v / self.total_records)
            except ZeroDivisionError:
                self.null_ratio[k] = float(0.0)

    def publish(self):
        self._null_ratio()

        self.record_gauge.labels(job=self.job, schema=self.schema).set(self.total_records)
        duration = time() - self.start_time
        self.processing_time.labels(job=self.job).set(duration)
        
        for field, count in self.null_count.items():
            self.null_gauge.labels(job=self.job, schema=self.schema, field=field).set(count)

        for field, count in self.null_ratio.items():
            self.ratio_gauge.labels(job=self.job, schema=self.schema,field=field).set(count)

        push_to_gateway(gateway=self.gateway, job=self.job, registry=self.registry)

    def clear(self, delay=15):
        def close():
            sleep(delay)
            delete_from_gateway(gateway=self.gateway, job=self.job)
        
        return threading.Thread(target=close).start()