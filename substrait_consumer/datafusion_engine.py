import json
import os
import time
from test_result import TestResult
import string
from pathlib import Path
from typing import Iterable

import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ds
from google.protobuf.json_format import Parse
from substrait.gen.proto.plan_pb2 import Plan

class DataFusionConsumer():

    def __init__(self, producer=None):
        self.ctx = SessionContext()

        if producer == 'Isthmus':
            os.system("mv data/tpch_parquet/lineitem.parquet data/tpch_parquet/LINEITEM.parquet")
            os.system("mv data/tpch_parquet/customer.parquet data/tpch_parquet/CUSTOMER.parquet")
            os.system("mv data/tpch_parquet/part.parquet data/tpch_parquet/PART.parquet")
            os.system("mv data/tpch_parquet/nation.parquet data/tpch_parquet/NATION.parquet")
            os.system("mv data/tpch_parquet/partsupp.parquet data/tpch_parquet/PARTSUPP.parquet")
            os.system("mv data/tpch_parquet/supplier.parquet data/tpch_parquet/SUPPLIER.parquet")
            os.system("mv data/tpch_parquet/orders.parquet data/tpch_parquet/ORDERS.parquet")
            os.system("mv data/tpch_parquet/region.parquet data/tpch_parquet/REGION.parquet")

        for table in os.listdir("/data/tpch_parquet/"):
            if table.endswith(".parquet"):
                self.ctx.register_parquet(f"{table.split('.')[0]}", f"/data/tpch_parquet/{table}")

        if producer == 'Isthmus':
            os.system("mv data/tpch_parquet/LINEITEM.parquet data/tpch_parquet/lineitem.parquet")
            os.system("mv data/tpch_parquet/CUSTOMER.parquet data/tpch_parquet/customer.parquet")
            os.system("mv data/tpch_parquet/PART.parquet data/tpch_parquet/part.parquet")
            os.system("mv data/tpch_parquet/NATION.parquet data/tpch_parquet/nation.parquet")
            os.system("mv data/tpch_parquet/PARTSUPP.parquet data/tpch_parquet/partsupp.parquet")
            os.system("mv data/tpch_parquet/SUPPLIER.parquet data/tpch_parquet/supplier.parquet")
            os.system("mv data/tpch_parquet/ORDERS.parquet data/tpch_parquet/orders.parquet")
            os.system("mv data/tpch_parquet/REGION.parquet data/tpch_parquet/region.parquet")


    def test_substrait(self, substrait_query, q, sf, producer):

        #print(substrait_query)

        if q == 'q18.sql':      # DataFusion thread panics at index out of bounds
            return None

        if producer == 'Isthmus':
            os.system("mv data/tpch_parquet/lineitem.parquet data/tpch_parquet/LINEITEM.parquet")
            os.system("mv data/tpch_parquet/customer.parquet data/tpch_parquet/CUSTOMER.parquet")
            os.system("mv data/tpch_parquet/part.parquet data/tpch_parquet/PART.parquet")
            os.system("mv data/tpch_parquet/nation.parquet data/tpch_parquet/NATION.parquet")
            os.system("mv data/tpch_parquet/partsupp.parquet data/tpch_parquet/PARTSUPP.parquet")
            os.system("mv data/tpch_parquet/supplier.parquet data/tpch_parquet/SUPPLIER.parquet")
            os.system("mv data/tpch_parquet/orders.parquet data/tpch_parquet/ORDERS.parquet")
            os.system("mv data/tpch_parquet/region.parquet data/tpch_parquet/REGION.parquet")

        times = []
        try:


            for i in range(4):
                stCPU = time.process_time()
                substrait_json = json.loads(substrait_query)
                plan_proto = Parse(json.dumps(substrait_json), Plan())
                plan_bytes = plan_proto.SerializeToString()
                substrait_plan = ds.substrait.serde.deserialize_bytes(plan_bytes)
                logical_plan = ds.substrait.consumer.from_substrait_plan(
                    self.ctx, substrait_plan
                )
                df_result = self.ctx.create_dataframe_from_logical_plan(logical_plan)
                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3
            res_obj = TestResult('TPC-H', 'Substrait', 'DataFusion', 'Parquet', producer, sf, q.split('.')[0],
                                 df_result.to_arrow_table(), times, timeAVG)
            print(f"TEST DataFusion\t\tSUCCESS")

            if producer == 'Isthmus':
                os.system("mv data/tpch_parquet/LINEITEM.parquet data/tpch_parquet/lineitem.parquet")
                os.system("mv data/tpch_parquet/CUSTOMER.parquet data/tpch_parquet/customer.parquet")
                os.system("mv data/tpch_parquet/PART.parquet data/tpch_parquet/part.parquet")
                os.system("mv data/tpch_parquet/NATION.parquet data/tpch_parquet/nation.parquet")
                os.system("mv data/tpch_parquet/PARTSUPP.parquet data/tpch_parquet/partsupp.parquet")
                os.system("mv data/tpch_parquet/SUPPLIER.parquet data/tpch_parquet/supplier.parquet")
                os.system("mv data/tpch_parquet/ORDERS.parquet data/tpch_parquet/orders.parquet")
                os.system("mv data/tpch_parquet/REGION.parquet data/tpch_parquet/region.parquet")

            return res_obj

        except Exception as e:
            print(f"TEST DataFusion\t\tEXCEPTION: Substrait not working: {repr(e)[:100]}")

            if producer == 'Isthmus':
                os.system("mv data/tpch_parquet/LINEITEM.parquet data/tpch_parquet/lineitem.parquet")
                os.system("mv data/tpch_parquet/CUSTOMER.parquet data/tpch_parquet/customer.parquet")
                os.system("mv data/tpch_parquet/PART.parquet data/tpch_parquet/part.parquet")
                os.system("mv data/tpch_parquet/NATION.parquet data/tpch_parquet/nation.parquet")
                os.system("mv data/tpch_parquet/PARTSUPP.parquet data/tpch_parquet/partsupp.parquet")
                os.system("mv data/tpch_parquet/SUPPLIER.parquet data/tpch_parquet/supplier.parquet")
                os.system("mv data/tpch_parquet/ORDERS.parquet data/tpch_parquet/orders.parquet")
                os.system("mv data/tpch_parquet/REGION.parquet data/tpch_parquet/region.parquet")

            return None

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