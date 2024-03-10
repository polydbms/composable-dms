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

class DataFusionParquetTester():

    def __init__(self):
        self.ctx = SessionContext()
        for table in os.listdir("/data/tpch_parquet/"):
            if table.endswith(".parquet"):
                self.ctx.register_parquet(f"{table.split('.')[0]}", f"/data/tpch_parquet/{table}")      #ToDo: Punctuation

    def test_substrait(self, substrait_query, q, sf, producer):

        if q == 'q18.sql':      # DataFusion thread panics at index out of bounds
            return None

        times = []
        try:
            substrait_json = json.loads(substrait_query)
            plan_proto = Parse(json.dumps(substrait_json), Plan())
            plan_bytes = plan_proto.SerializeToString()
            substrait_plan = ds.substrait.serde.deserialize_bytes(plan_bytes)
            logical_plan = ds.substrait.consumer.from_substrait_plan(
                self.ctx, substrait_plan
            )
            for i in range(4):
                stCPU = time.process_time()                                            #ToDo: Clarify measurement Scope
                df_result = self.ctx.create_dataframe_from_logical_plan(logical_plan)
                etCPU = time.process_time()
                resCPU = (etCPU - stCPU) * 1000
                if (i == 1) | (i == 2) | (i == 3):
                    times.append(resCPU)
            timeAVG = (times[0] + times[1] + times[2]) / 3
            res_obj = TestResult('TPC-H', 'DataFusion', 'Parquet', producer, sf, q.split('.')[0],
                                 df_result.to_arrow_table(), times, timeAVG)
            print(f"TEST DataFusion\t\tSUCCESS")
            return res_obj

        except Exception as e:
            print(f"TEST DataFusion\t\tEXCEPTION: Substrait not working: {repr(e)}")
            return None
