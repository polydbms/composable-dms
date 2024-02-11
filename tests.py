import os
import json
import pyarrow as pa
from pyarrow.lib import tobytes
import pyarrow.substrait as substrait
from pyarrow import csv
from datafusion import SessionContext
from datafusion import substrait as ss
#import pandas as pd

#with open('data/temp/substrait_queries.json', 'r') as f:
#    sq_data = json.load(f)

#print("SQ Data")
#print(sq_data)

ctx = SessionContext()
ctx.register_csv(
    "sf1lineitem", "/data/sf1/lineitem.csv"
)

with open("data/temp/sf1q1.proto", 'rb') as f:
    proto = f.read()

substrait_plan = ss.substrait.serde.deserialize_bytes(proto)

df_logical_plan = ss.substrait.consumer.from_substrait_plan(ctx, substrait_plan)

results = ctx.create_dataframe_from_logical_plan(df_logical_plan)
print(results)

