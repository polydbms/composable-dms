import os
import duckdb
import time
from test_result import TestResult


class DuckDBConsumer():

    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()

        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")

        for table in os.listdir("/data/tpch_parquet/"):
            if table.endswith(".parquet"):
                self.db_connection.execute(f"CREATE VIEW {table.split('.')[0]} AS SELECT * FROM read_parquet('/data/tpch_parquet/{table}')")


    def test_substrait(self, substrait_query, q, sf, producer):

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
            # print(query_result)
            res_obj = TestResult('TPC-H', 'Substrait', 'DuckDB', 'Parquet', producer, sf, q.split('.')[0], query_result, times, timeAVG)
            print(f"TEST DuckDB\t\tSUCCESS")
            return res_obj

        except Exception as e:
            print(f"TEST DuckDB\t\tEXCEPTION: Substrait not working: {repr(e)}")
            return None

    def test_sql(self, sql_query, q, sf):

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
            # print(query_result)
            res_obj = TestResult('TPC-H', 'SQL', 'DuckDB', 'Parquet', '-- SQL', sf, q.split('.')[0], query_result,
                                 times, timeAVG)
            print(f"TEST DuckDB\t\tSUCCESS")
            return res_obj

        except Exception as e:
            print(f"TEST DuckDB\t\tEXCEPTION: SQL not working: {repr(e)}")
            return None


    def print_db(self):
        self.db_connection.sql("SELECT * FROM information_schema.tables").show()