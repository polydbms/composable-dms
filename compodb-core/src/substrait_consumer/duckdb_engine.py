import os
import duckdb
from duckdb import DuckDBPyRelation
from typing import Optional
import time
from src.substrait_consumer.consumer import Consumer
from src.errors import ConsumptionError


class DuckDBConsumer(Consumer):

    def __init__(self):
        self.db_connection = duckdb.connect()
        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")


    def run_substrait(self, substrait_query) -> Optional[DuckDBPyRelation]:

        try:
            query_result = self.db_connection.from_substrait_json(substrait_query)
            return query_result
        except Exception as e:
            self.db_connection.close()
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")
            raise ConsumptionError(repr(e))


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

    def register_table(self, table: str):
        view_name = table.split('.')[0]

        self.db_connection.execute(f"DROP VIEW IF EXISTS {view_name}")
        if table.endswith(".parquet"):
            self.db_connection.execute(
                f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('/data/parquet/{table}')"
            )
        else:
            self.db_connection.execute(
                f"CREATE VIEW {view_name} AS SELECT * FROM read_csv('/data/csv/{table}')"
            )


    def print_db(self):
        self.db_connection.sql("SELECT * FROM information_schema.tables").show()


    def get_name(self) -> str:
        return "DuckDB"