from typing import List


class BenchmarkResult:

    parser_name: str = None
    optimizer: List[str] = []
    execution_engine_name: str = None
    data_format: str = None
    scale_factor = None
    query_name: str = None
    substrait_query: str = None
    query_result = None
    measurements = []
    runtime: float  = None
    error_msg = None


    def __init__(self, parser_name, optimizer, execution_engine_name, data_format, scale_factor):
        self.parser_name = parser_name
        self.optimizer = optimizer
        self.execution_engine_name = execution_engine_name
        self.data_format = data_format
        self.scale_factor = scale_factor


    def add_failure(self, e) -> None:
        self.error_msg = e


    def json(self):
        return {
            "parser_name": self.parser_name,
            "optimizer": self.optimizer,
            "execution_engine_name": self.execution_engine_name,
            "data_format": self.data_format,
            "scale_factor": self.scale_factor,
            "query_name": self.query_name,
            "substrait_query": self.substrait_query,
            "query_result": self.query_result,
            "measurements": self.measurements,
            "runtime": self.runtime,
            "error_msg": self.error_msg
        }