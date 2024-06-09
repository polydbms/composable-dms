import os
import string
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.substrait as substrait
import time
from test_result import TestResult
from times import Times


class AceroConsumer():

    def __init__(self):
        self.tables = {}
        self.table_provider = lambda names, schema: self.tables[names[0].lower()]

    def substrait(self, substrait_query):
        times = []
        try:
            for i in range(4):
                stCPU = time.process_time()
                encoded_substrait = substrait_query.encode()
                substrait_query = pa._substrait._parse_json_plan(encoded_substrait)
                reader = substrait.run_query(
                    substrait_query, table_provider=self.table_provider
                )
                query_result = reader.read_all()
                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3

            times_obj = Times(times, timeAVG)

            return query_result, times_obj

        except Exception as e:
            return None, e

    def register_tables(self):
        data_path = "/data/tpch_parquet/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                table_name = Path(file).stem
                table_name = table_name.translate(
                    str.maketrans("", "", string.punctuation)
                )
                self.tables[table_name] = pq.read_table(data_path+file)
