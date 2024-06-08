from times import Times


class TestResult:
    def __init__(self, producer, engine, query, query_set, query_name, sf, data_format, query_result, times: Times):
        self.query_set = query_set
        self.query = query
        self.engine = engine
        self.data_format = data_format
        self.producer = producer
        self.sf = sf
        self.query_name = query_name
        self.query_result = query_result
        self.times = times

    def __str__(self):
        return (f"Test Result of the {self.query_set} {self.query_name} query executed on {self.engine} based on SF{self.sf} {self.data_format} files:\n"
                f" Producer: {self.producer}\n"
                f" Consumer: {self.engine}\n"
                f" Measurements:\t{self.times.measurements}\n Runtime: \t{self.times.runtime} ms\n")

    def row(self):
        return