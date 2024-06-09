import os
import duckdb
import time
from test_result import TestResult
from times import Times


class DuckDBConsumer():

    def __init__(self):
        self.db_connection = duckdb.connect()
        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")

        for table in os.listdir("/data/tpch_parquet/"):
            if table.endswith(".parquet"):
                self.db_connection.execute(f"CREATE VIEW {table.split('.')[0]} AS SELECT * FROM read_parquet('/data/tpch_parquet/{table}')")

    def substrait(self, substrait_query):
        times = []
        try:
            for i in range(4):
                stCPU = time.process_time()
                query_result = self.db_connection.from_substrait_json(substrait_query)
                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3

            times_obj = Times(times, timeAVG)

            return query_result, times_obj

        except Exception as e:
            return None, e

    def sql(self, sql_query):
        times = []
        try:
            for i in range(4):
                stCPU = time.process_time()
                query_result = self.db_connection.sql(sql_query)
                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3

            times_obj = Times(times, timeAVG)

            return query_result, times_obj

        except Exception as e:
            return None, e

    def print_db(self):
        self.db_connection.sql("SELECT * FROM information_schema.tables").show()