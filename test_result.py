
class TestResult:
    def __init__(self, benchmark, query, engine, format, producer, sf, q, query_result, measurements, runtime):
        self.benchmark = benchmark
        self.query = query
        self.engine = engine
        self.format = format
        self.producer = producer
        self.sf = sf
        self.q = q
        self.query_result = query_result
        self.measurements = measurements
        self.runtime = runtime

    def __str__(self):
        return (f"Test Result of {self.benchmark} {self.query} query {self.q} on {self.engine} based on {self.format} files:\n"
                f" Producer: {self.producer}\n"
                f" Consumer: {self.engine}\n"
                f" Measurements:\t{self.measurements}\n Runtime: \t{self.runtime} ms\n")