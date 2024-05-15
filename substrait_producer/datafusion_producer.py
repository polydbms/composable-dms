import os
import pyarrow as pa
import json
from datafusion import SessionContext
from datafusion import substrait as ds
import substrait.gen.proto.plan_pb2 as plan_pb2
from google.protobuf.json_format import MessageToJson

class DataFusionProducer:
    def __init__(self):
        self.ctx = SessionContext()

        for table in os.listdir("/data/tpch_parquet/"):
            if table.endswith(".parquet"):
                self.ctx.register_parquet(f"{table.split('.')[0]}", f"/data/tpch_parquet/{table}")

    def produce_substrait(self, query, filename, q_set):
        try:
            substrait_proto = plan_pb2.Plan()
            substrait_plan = ds.substrait.serde.serialize_to_plan(query, self.ctx)
            substrait_plan_bytes = substrait_plan.encode()
            substrait_proto.ParseFromString(substrait_plan_bytes)
            file_name = f"/data/substrait_plans/substrait_{q_set.split('_')[2]}_datafusion_{filename.split('.')[0]}.json"
            #with open(file_name, "w") as outfile:
            #    outfile.write(json.dumps(MessageToJson(substrait_proto)))
            print(f"PROD DataFusion\t\tPROD SUCCESS")
            return MessageToJson(substrait_proto)

        except Exception as e:
            print(f"PROD DataFusion\t\tPROD EXCEPTION: {filename.split('.')[0]}: {repr(e)}")