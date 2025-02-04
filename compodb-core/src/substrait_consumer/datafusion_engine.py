import json
import os
import time
import string
from pathlib import Path
from typing import Iterable
import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ds
from datafusion import DataFrame
from src.substrait_consumer.execution_engine import ExecutionEngine
from src.substrait_producer.isthmus_producer import IsthmusProducer
from src.substrait_producer.duckdb_producer import DuckDBProducer
from src.substrait_producer.isthmus_kit.tpch_schema import (lineitem_schema, customer_schema, nation_schema,
                                                            orders_schema, part_schema, partsupp_schema, region_schema,
                                                            supplier_schema)
from google.protobuf.json_format import Parse
from substrait.gen.proto.plan_pb2 import Plan
from typing import Optional
from src.errors import ConsumptionError
import logging
import re

logger = logging.getLogger()


class DataFusionEngine(ExecutionEngine):

    def __init__(self):
        self.ctx = SessionContext()
        self.table_mapping = {}
        self.schema_mapping = {
            'lineitem': lineitem_schema,
            'customer': customer_schema,
            'nation': nation_schema,
            'orders': orders_schema,
            'part': part_schema,
            'partsupp': partsupp_schema,
            'region': region_schema,
            'supplier': supplier_schema
        }

    def register_table(self, table: str):
        self.table_mapping[table.split('.')[0].upper()] = table.split('.')[0]
        try:
            schema = self.schema_mapping.get(table.split('.')[0])
            if table.endswith(".parquet"):
                self.ctx.register_parquet(f"{table.split('.')[0]}", f"/app/data/parquet/{table}", schema=schema)
            else:
                self.ctx.register_csv(f"{table.split('.')[0]}", f"/app/data/csv/{table}", schema=schema)
        except Exception as e:
            if "already exists" in str(e):
                self.ctx.deregister_table(table.split('.')[0])
                schema = self.schema_mapping.get(table.split('.')[0])
                if table.endswith(".parquet"):
                    self.ctx.register_parquet(f"{table.split('.')[0]}", f"/data/parquet/{table}", schema=schema)
                else:
                    self.ctx.register_csv(f"{table.split('.')[0]}", f"/data/csv/{table}", schema=schema)



    def prep_isthmus_query(self, substrait_query: str) -> str:

        for upper_name, lower_name in self.table_mapping.items():
            substrait_query = re.sub(r'\b' + re.escape(upper_name) + r'\b', lower_name, substrait_query)

            column_df = self.ctx.sql(f"SHOW COLUMNS FROM {lower_name}").to_pandas()
            column_names = column_df["column_name"].tolist()

            for col in column_names:
                substrait_query = substrait_query.replace(col.upper(), col)

        return substrait_query


    def run_substrait(self, substrait_query) -> Optional[DataFrame]:

        if isinstance(self.compodb.parser, IsthmusProducer):
            substrait_query = self.prep_isthmus_query(substrait_query)

        try:
            substrait_json = json.loads(substrait_query)
            plan_proto = Parse(json.dumps(substrait_json), Plan())
            plan_bytes = plan_proto.SerializeToString()
            substrait_plan = ds.serde.deserialize_bytes(plan_bytes)
            logical_plan = ds.consumer.from_substrait_plan(
                self.ctx, substrait_plan
            )
            result = self.ctx.create_dataframe_from_logical_plan(logical_plan)
            result = result.collect()
            return result

        except Exception as e:
            raise ConsumptionError(repr(e))


    def test_sql(self, sql_query, q, sf):

        times = []
        try:
            for i in range(4):
                stCPU = time.process_time()

                query_result = self.ctx.sql(sql_query)

                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3
            # print(query_result)
            res_obj = TestResult('TPC-H', 'SQL', 'DataFusion', 'Parquet', '-- SQL', sf, q.split('.')[0], query_result,
                                 times, timeAVG)
            print(f"TEST DataFusion\t\tSUCCESS")

            return res_obj

        except Exception as e:
            print(f"TEST DataFusion\t\tEXCEPTION: SQL not working: {repr(e)[:100]}")
            return None


    def get_name(self) -> str:
        return "DataFusion"