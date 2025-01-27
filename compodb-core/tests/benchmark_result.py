

class BenchmarkResult:

    producer_name: str = None
    consumer_name: str = None
    data_format: str = None
    scale_factor = None
    query_name: str = None
    substrait_query: str = None
    query_result = None
    measurements = []
    runtime: float  = None
    error_msg = None


    def __init__(self, producer_name, consumer_name, data_format, scale_factor, query_name):
        self.producer_name = producer_name
        self.consumer_name = consumer_name
        self.data_format = data_format
        self.scale_factor = scale_factor
        self.query_name = query_name


    def add_failure(self, e) -> None:
        self.error_msg = e


    def json(self):
        return {
            "producer_name": self.producer_name,
            "consumer_name": self.consumer_name,
            "data_format": self.data_format,
            "scale_factor": self.scale_factor,
            "query_name": self.query_name,
            "substrait_query": self.substrait_query,
            "query_result": self.query_result,
            "measurements": self.measurements,
            "runtime": self.runtime,
            "error_msg": self.error_msg
        }