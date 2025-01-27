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
from src.substrait_consumer.consumer import Consumer
from src.substrait_producer.isthmus_producer import IsthmusProducer
from google.protobuf.json_format import Parse
from substrait.gen.proto.plan_pb2 import Plan
from typing import Optional
from src.errors import ConsumptionError


def change_file_case(flag):
    """
    Change the case of filenames in specific directories based on the flag. This is necessary because of some specific
    properties the Isthmus Optimizer and the DataFusion Engines combination have.

    :param flag: 'uppercase' to make filenames uppercase, 'lowercase' to make filenames lowercase.
    """
    paths_and_extensions = {
        '/data/parquet': '.parquet',
        '/data/csv': '.csv'
    }

    for path, extension in paths_and_extensions.items():
        if not os.path.exists(path):
            print(f"Path not found: {path}")
            continue

        for filename in os.listdir(path):
            if filename.endswith(extension):
                old_path = os.path.join(path, filename)
                name_part, ext_part = os.path.splitext(filename)

                if flag == 'uppercase':
                    new_filename = name_part.upper() + ext_part
                elif flag == 'lowercase':
                    new_filename = name_part.lower() + ext_part
                else:
                    raise ValueError("Flag must be 'uppercase' or 'lowercase'")

                new_path = os.path.join(path, new_filename)

                if old_path != new_path:
                    os.rename(old_path, new_path)
                    print(f"Renamed: {old_path} -> {new_path}")


class DataFusionConsumer(Consumer):

    def __init__(self):
        self.ctx = SessionContext()


    def register_table(self, table: str):
        if isinstance(self.compodb.producer, IsthmusProducer):
            change_file_case('uppercase')
            name_part, ext_part = os.path.splitext(table)
            table = name_part.upper() + ext_part

        try:
            if table.endswith(".parquet"):
                self.ctx.register_parquet(f"{table.split('.')[0]}", f"/data/parquet/{table}")
            else:
                self.ctx.register_csv(f"{table.split('.')[0]}", f"/data/csv/{table}")
        except Exception as e:
            if "already exists" in str(e):
                self.ctx.deregister_table(table)

                if table.endswith(".parquet"):
                    self.ctx.register_parquet(f"{table.split('.')[0]}", f"/data/parquet/{table}")
                else:
                    self.ctx.register_csv(f"{table.split('.')[0]}", f"/data/csv/{table}")


        if isinstance(self.compodb.producer, IsthmusProducer):
            change_file_case('lowercase')


    def run_substrait(self, substrait_query) -> Optional[DataFrame]:

        # if q == 'q18.sql':    TODO: DataFusion thread panics at index out of bounds !!! Exclude before f-call
        #     return None

        if isinstance(self.compodb.producer, IsthmusProducer):
            change_file_case('uppercase')

        try:
            substrait_json = json.loads(substrait_query)
            plan_proto = Parse(json.dumps(substrait_json), Plan())
            plan_bytes = plan_proto.SerializeToString()
            substrait_plan = ds.substrait.serde.deserialize_bytes(plan_bytes)
            logical_plan = ds.substrait.consumer.from_substrait_plan(
                self.ctx, substrait_plan
            )
            result = self.ctx.create_dataframe_from_logical_plan(logical_plan)

            if isinstance(self.compodb.producer, IsthmusProducer):
                change_file_case('lowercase')

            return result

        except Exception as e:
            if isinstance(self.compodb.producer, IsthmusProducer):
                change_file_case('lowercase')

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