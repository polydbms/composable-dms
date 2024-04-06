import os
import string
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.substrait as substrait
import time
from test_result import TestResult

class AceroConsumer():

    def __init__(self):
        self.tables = {}
        self.table_provider = lambda names, schema: self.tables[names[0].lower()]
        data_path = "/data/tpch_parquet/"
        for file in os.listdir(data_path):
            if file.endswith(".parquet"):
                table_name = Path(file).stem
                table_name = table_name.translate(
                    str.maketrans("", "", string.punctuation)
                )
                #print("ACERO table path:")
                #print(data_path+file)
                self.tables[table_name] = pq.read_table(data_path+file)

    def test_substrait(self, substrait_query, q, sf, producer):

        #print(substrait_query)
        times = []
        encoded_substrait = substrait_query.encode()

        try:
            for i in range(4):
                stCPU = time.process_time()

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
            # print(query_result)
            res_obj = TestResult('TPC-H', 'Substrait', 'Acero', 'Parquet', producer, sf, q.split('.')[0], query_result, times,
                                 timeAVG)
            print(f"TEST Acero\t\tSUCCESS")

            return res_obj

        except Exception as e:
            print(f"TEST Acero\t\tEXCEPTION: Substrait not working: {repr(e)[:100]}")
            return None
