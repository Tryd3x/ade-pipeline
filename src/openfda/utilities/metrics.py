import threading
from time import time, sleep
from urllib.error import URLError 
from openfda.utilities.helper import get_module_logger
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway, delete_from_gateway

logger = get_module_logger(__name__)

class Metrics:
    schema = ['patients', 'drugs', 'reactions']
    def __init__(self, job, gateway):
        logger.info("Initializing Metrics")
        self.start_time = time()
        self.total_records = {k:0 for k in self.schema}
        self.null_count = {k:{} for k in self.schema}
        self.null_ratio = {k:{} for k in self.schema}

        self.registry = CollectorRegistry()
        self.gateway = gateway
        self.job = job

        # Metrics
        self.record_gauge = Gauge('total_records', 'Total records per table', ['job','table'], registry=self.registry)
        self.null_gauge = Gauge('null_count', 'Null count per field', ['job','table', 'field'], registry=self.registry)
        self.ratio_gauge = Gauge('null_ratio', 'Null ratio per field', ['job','table', 'field'], registry=self.registry)
        self.processing_time = Gauge('batch_processing_time', 'Time taken to process a batch in seconds', ['job'],registry=self.registry)

    def reset(self):
        self.start_time = time()
        self.total_records = {k:0 for k in self.schema}
        self.null_count = {k:{} for k in self.schema}
        self.null_ratio = {k:{} for k in self.schema}

    def update(self, ade):
        """ Update total_records and null_count"""
        patients_df, drugs_df, reactions_df = ade._to_dataframe()

        for s, df in zip(self.schema, [patients_df, drugs_df, reactions_df]):
            # Calculate total records
            self.total_records[s] = self.total_records[s] + df.shape[0]

            # Calculate null counts
            null_count = df.isna().sum().to_dict()
            for k, v in null_count.items():
                self.null_count[s][k] = self.null_count[s].get(k, 0) + v
    
    def _null_ratio(self):
        """ Update null ratio"""
        for s in self.schema:
            for k,v in self.null_count[s].items():
                try:
                    self.null_ratio[s][k] = float(v / self.total_records[s])
                except ZeroDivisionError:
                    self.null_ratio[s][k] = float(0.0)

    def publish(self,):
        """ Pulish updated metrics to prometheus"""

        # Invoke _null_ratio
        self._null_ratio()

        # Processing time
        duration = time() - self.start_time

        # Update Gauge
        for table, count in self.total_records.items():
            self.record_gauge.labels(job=self.job,table=table).set(count)

        for table, fields in self.null_count.items():
            for field, count in fields.items():
                self.null_gauge.labels(job=self.job,table=table, field=field).set(count)

        for table, fields in self.null_ratio.items():
            for field, ratio in fields.items():
                self.ratio_gauge.labels(job=self.job,table=table, field=field).set(ratio)
        
        self.processing_time.labels(job=self.job).set(duration)

        logger.info(f"Posting metrics to pushgateway: {self.gateway}")
        try:
            push_to_gateway(gateway=self.gateway,job=self.job,registry=self.registry)
        except URLError:
            logger.warning(f"Failed to push metrics to gateway: {self.gateway}")

    def close(self, delay=15):
        logger.info(f"Clearing Metrics")
        def close():
            sleep(delay)
            try:
                delete_from_gateway(gateway=self.gateway, job=self.job)
            except URLError:
                logger.warning(f"Failed to delete metrics at gateway: {self.gateway}")
        
        threading.Thread(target=close).start()