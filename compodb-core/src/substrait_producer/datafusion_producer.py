import os
import json
from src.substrait_producer.parser import Parser
from src.substrait_producer.optimizer import Optimizer
import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ds
import substrait.gen.proto.plan_pb2 as plan_pb2
from google.protobuf.json_format import MessageToJson
from src.errors import ProductionError


class DataFusionProducer(Parser, Optimizer):

    def __init__(self):
        self.ctx = SessionContext()


    def to_substrait(self, native_query) -> str:
        try:
            substrait_proto = plan_pb2.Plan()
            substrait_plan = ds.serde.serialize_to_plan(native_query, self.ctx)
            substrait_plan_bytes = substrait_plan.encode()
            substrait_proto.ParseFromString(substrait_plan_bytes)
            return MessageToJson(substrait_proto)

        except Exception as e:
            raise ProductionError(repr(e))


    def optimize_substrait(self, substrait_query: str) -> str:
        return substrait_query


    def register_table(self, table: str) -> None:
        table_name = table.split('.')[0]  # Extract table name

        # Attempt to register the table
        try:
            if table.endswith(".parquet"):
                self.ctx.register_parquet(table_name, f"/data/parquet/{table}")
            else:
                self.ctx.register_csv(table_name, f"/data/csv/{table}")
        except Exception as e:
            # Handle the "table already exists" error
            if "already exists" in str(e):
                self.ctx.deregister_table(table_name)  # Deregister the table if it exists
                # Re-register the table after deregistering
                if table.endswith(".parquet"):
                    self.ctx.register_parquet(table_name, f"/data/parquet/{table}")
                else:
                    self.ctx.register_csv(table_name, f"/data/csv/{table}")
            else:
                # Raise the exception if it's not related to "already exists"
                raise


    def get_name(self) -> str:
        return "DataFusion"
