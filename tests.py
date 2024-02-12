import os
import json
import pyarrow as pa
from pyarrow.lib import tobytes
import pyarrow.substrait as substrait
from pyarrow import csv
import re
from datafusion import SessionContext
from datafusion import substrait as ss
import pandas as pd


# Get datafusion SessionContext obj
def getSessionContextDF():
    ctx = SessionContext()
    # Register tables
    for sf in os.listdir("/data"):
        if sf.startswith("sf"):
            ctx.register_csv(f"{sf}orders", f"/data/{sf}/orders.csv")
            ctx.register_csv(f"{sf}nation", f"/data/{sf}/nation.csv")
            ctx.register_csv(f"{sf}lineitem", f"/data/{sf}/lineitem.csv")
            ctx.register_csv(f"{sf}region", f"/data/{sf}/region.csv")
            ctx.register_csv(f"{sf}supplier", f"/data/{sf}/supplier.csv")
            ctx.register_csv(f"{sf}partsupp", f"/data/{sf}/partsupp.csv")
            ctx.register_csv(f"{sf}part", f"/data/{sf}/part.csv")
            ctx.register_csv(f"{sf}customer", f"/data/{sf}/customer.csv")
    return ctx




# Get SQL Query - for testing
def getQuery(sf, q):
    # Define sf substitution param
    reg = re.compile(r"\$")
    # Substitute table names with corresponding sf-table names
    with open(f'/queries/{q}') as file:
        subquery = re.sub(reg, sf, re.sub(reg, sf, re.sub(reg, sf, re.sub(reg, sf, re.sub(reg, sf, re.sub(reg,
                    sf, re.sub(reg, sf, re.sub(reg, sf, file.read()))))))))
    return subquery

def runAceroTest(query):
    lineitem = pa.csv.read_csv(f"/data/sf1/lineitem.csv")

    def table_provider(names, schema):
        if not names:
            raise Exception("No names provided")
        elif names[0] == "lineitem":
            return lineitem
        else:
            raise Exception("Unrecognized table name")

    substrait_bytes = None
    with open(f"/data/substrait/proto/test_q.proto", "rb") as f:
        substrait_bytes = f.read()

    reader = pa.substrait.run_query(
        substrait_bytes, table_provider=table_provider
    )
    result = reader.read_all().to_pandas()
    print("Acero test result:")
    print(result)




if __name__ == "__main__":

    # |-------|
    # | Acero |
    # |-------|

    print("\n\n\tConsuming with Acero:\n")

    for sf in os.listdir("/data"):
        if sf.startswith("sf"):
            orders = pa.csv.read_csv(f"/data/{sf}/orders.csv")
            nation = pa.csv.read_csv(f"/data/{sf}/nation.csv")
            lineitem = pa.csv.read_csv(f"/data/{sf}/lineitem.csv")
            region = pa.csv.read_csv(f"/data/{sf}/region.csv")
            supplier = pa.csv.read_csv(f"/data/{sf}/supplier.csv")
            partsupp = pa.csv.read_csv(f"/data/{sf}/partsupp.csv")
            part = pa.csv.read_csv(f"/data/{sf}/part.csv")
            customer = pa.csv.read_csv(f"/data/{sf}/customer.csv")

            def table_provider(names, schema):
                if not names:
                    raise Exception("No names provided")
                elif names[0] == f"{sf}orders":
                    return orders
                elif names[0] == f"{sf}nation":
                    return nation
                elif names[0] == f"{sf}lineitem":
                    return lineitem
                elif names[0] == f"{sf}region":
                    return region
                elif names[0] == f"{sf}supplier":
                    return supplier
                elif names[0] == f"{sf}partsupp":
                    return partsupp
                elif names[0] == f"{sf}part":
                    return part
                elif names[0] == f"{sf}customer":
                    return customer
                else:
                    raise Exception("Unrecognized table name")

            for p_sf in os.listdir("/data/substrait/proto"):
                if p_sf.startswith(sf):
                    try:
                        substrait_bytes = None
                        with open(f"/data/substrait/proto/{p_sf}", "rb") as f:
                            substrait_bytes = f.read()

                        reader = pa.substrait.run_query(
                            substrait_bytes, table_provider=table_provider
                        )
                        result = reader.read_all().to_pandas()
                        print(f"Acero result on {p_sf.split('_')[1].upper()}:")
                        print(result)
                    except Exception as e:
                        print(f"EXCEPTION with {p_sf.split('_')[1].upper()} on {p_sf.split('_')[0]}:")
                        print(f" Acero Error: {repr(e)}\n")


    #with open('data/temp/substrait_queries.json', 'r') as f:
    #    sq_data = json.load(f)

    #print("SQ Data")
    #print(sq_data)

    # |------------|
    # | Datafusion |
    # |------------|

    print("\n\n\tConsuming with Datafusion:\n")

    # Get datafusion context obj
    ctx = getSessionContextDF()

    # Get query-protobuf and execute
    for pf in os.listdir("/data/substrait/proto"):
        if not (pf.split('_')[1] == 'q18'):
            with open(f"data/substrait/proto/{pf}", 'rb') as f:
                proto_q = f.read()

            substrait_plan = ss.substrait.serde.deserialize_bytes(proto_q)

            try:
                df_logical_plan = ss.substrait.consumer.from_substrait_plan(ctx, substrait_plan)
                results = ctx.create_dataframe_from_logical_plan(df_logical_plan)
                print(f"Datafusion: {pf.split('_')[1].upper()} on {pf.split('_')[0]}:")
                print(results)
            except Exception as e:
                print(f"EXCEPTION with {pf.split('_')[1].upper()} on {pf.split('_')[0]}:")
                print(f" Datafusion Error: {repr(e)}\n")


    # Test plain sql query on Acero
    print("SQL test Acero:")
    query = getQuery('sf1', 'q1.sql')
    print(query)
    runAceroTest(query)

    # Test plain sql query on datafusion
    #print("SQL test Datafusion:")
    #query = getQuery('sf1', 'q1.sql')
    #print(query)
    #df = ctx.sql(query)
    #df.to_pandas()

    print("\n\tbenchmark_test completed")

