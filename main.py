import os
#import pandas as pd
import duckdb
#import time
#import re
import matplotlib.pyplot as plt
import numpy as np
#import json

import pyarrow as pa
from pyarrow.lib import tobytes
import pyarrow.substrait as substrait
from pyarrow import csv
import pyarrow.parquet as pq
from datafusion import SessionContext
from datafusion import substrait as ss
from pathlib import Path
from filelock import FileLock
from test_result import TestResult

from substrait_producer import duckdb_producer, ibis_producer, isthmus_producer
from substrait_consumer import duckdb_parquet_tester, datafusion_parquet_tester, acero_parquet_tester


def get_sql_query(q):
    with open(f'/queries/tpch_sql/{q}') as file:
        query = file.read()
    return query

def plotResults(results, sf_plot):
    queries = ()
    for i in range(1, 23):
        count = True
        for res in results:
            if res.q[1:] == str(i):
                if count: queries += (res.q.upper(),)
                count = False
                sf_plot[res.sf] += (round(res.runtime, 2),)

    x = np.arange(len(queries))
    width = 0.4
    multiplier = 0

    fig, ax = plt.subplots(figsize=(20,10))

    for sf_attr, measurement in sf_plot.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, measurement, width, label=sf_attr)
        ax.bar_label(rects, padding=3)
        multiplier += 1

    ax.set_ylabel('CPU runtime in ms')
    ax.set_title('TPC-H Queries on DuckDB')
    ax.set_xticks(x+(width/2), queries)
    ax.legend(loc='upper left', ncols=len(sf_plot))
    ax.set_ylim(0, 22)

    plt.savefig("/data/results/plots/tpch_plot.png")

def get_isthmus_schema():
    isthmus_schema = []
    for file in os.listdir("/substrait_producer/isthmus_kit"):
        if file.endswith(".sql"):
            with open(f'/substrait_producer/isthmus_kit/{file}') as create_file:
                create_sql = create_file.read()
            isthmus_schema.append(create_sql)

    return isthmus_schema

def create_tpch_data(scale_factor=0.1):

    data_path = Path(__file__).parent / "data" / "tpch_csv"
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf={scale_factor})")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT CSV);")
        con.close()

    data_path = Path(__file__).parent / "data" / "tpch_parquet"
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf={scale_factor})")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")
        con.close()



if __name__ == "__main__":
    print("\n\tExecution Engine Benchmark Test\n")

    # Create tables
    sf = 0.1
    create_tpch_data(sf)      #ToDo: Args

    # Init
    results = []    # list[TestResult]
    isthmus_schema_list = get_isthmus_schema()

    # Init Producers
    duckdb_prod = duckdb_producer.DuckDBProducer(sf)
    ibis_prod = ibis_producer.IbisProducer(sf, duckdb_prod.db_connection)
    isthmus_prod = isthmus_producer.IsthmusProducer(sf)

    # Init Tester
    duckdb_parquet_cons = duckdb_parquet_tester.DuckDBParquetTester()
    datafusion_parquet_cons = datafusion_parquet_tester.DataFusionParquetTester()
    acero_parquet_cons = acero_parquet_tester.AceroConsumer()


    # SQL
    for q in os.listdir("/queries/tpch_sql"):

        print("\n--------------------------------------------------------------------------")
        print(f"\n\t{q.split('.')[0].upper()}:\n")

        sql_query = get_sql_query(q)
        duckdb_query = duckdb_prod.produce_substrait(sql_query, q)
        ibis_query = ibis_prod.produce_substrait(q)
        isthmus_query = isthmus_prod.produce_substrait(isthmus_schema_list, sql_query)


        # Format: consumer_producer_format_result

        if duckdb_query is not None:
            print("\n\nPRODUCER DuckDB:\n")
            duckdb_duckdb_parquet_result = duckdb_parquet_cons.test_substrait(duckdb_query, q, sf, 'DuckDB')
            if duckdb_duckdb_parquet_result is not None: results.append(duckdb_duckdb_parquet_result)

            datafusion_duckdb_parquet_result = datafusion_parquet_cons.test_substrait(duckdb_query, q, sf, 'DuckDB')
            if datafusion_duckdb_parquet_result is not None: results.append(datafusion_duckdb_parquet_result)

            acero_duckdb_parquet_result = acero_parquet_cons.test_substrait(duckdb_query, q, sf, 'DuckDB')
            if acero_duckdb_parquet_result is not None: results.append(acero_duckdb_parquet_result)

        if ibis_query is not None:
            print("\n\nPRODUCER Ibis:\n")
            duckdb_ibis_parquet_result = duckdb_parquet_cons.test_substrait(ibis_query, q, sf, 'Ibis')
            if duckdb_ibis_parquet_result is not None: results.append(duckdb_ibis_parquet_result)

            datafusion_ibis_parquet_result = datafusion_parquet_cons.test_substrait(ibis_query, q, sf, 'Ibis')
            if datafusion_ibis_parquet_result is not None: results.append(datafusion_ibis_parquet_result)

            acero_ibis_parquet_result = acero_parquet_cons.test_substrait(ibis_query, q, sf, 'Ibis')
            if acero_ibis_parquet_result is not None: results.append(acero_ibis_parquet_result)

        if isthmus_query is not None:
            print("\n\nPRODUCER Isthmus:\n")
            duckdb_isthmus_parquet_result = duckdb_parquet_cons.test_substrait(isthmus_query, q, sf, 'Isthmus')
            if duckdb_isthmus_parquet_result is not None: results.append(duckdb_isthmus_parquet_result)

            datafusion_isthmus_parquet_result = datafusion_parquet_cons.test_substrait(isthmus_query, q, sf, 'Isthmus')
            if datafusion_isthmus_parquet_result is not None: results.append(datafusion_isthmus_parquet_result)

            acero_isthmus_parquet_result = acero_parquet_cons.test_substrait(isthmus_query, q, sf, 'Isthmus')
            if acero_isthmus_parquet_result is not None: results.append(acero_isthmus_parquet_result)




    # Print results
    print("\n\n")
    for r in results:
        print(r.__str__())


'''
    sf_plot = {}
    tab_rel = con.sql("SELECT table_name FROM duckdb_tables();").df()
    print('Checking if data already exists..')
    # Create csv - Views
    for sf in os.listdir("/data"):
        if sf.startswith("sf"):                   # Temp
            sf_plot.update({sf: ()})
            csv = False
            tables = False
            for f in os.listdir(f"/data/{sf}"):
                if f == "customer.csv":
                    csv = True
                    print(f" --> csv data found for {sf}")
            for t in tab_rel['table_name']:
                if t.startswith(sf):
                    tables = True
            if not csv:
                createCSV(sf)
                #createPARQUET()
            if not tables:
                insertDuckDB(con, sf)
                #createDDBView(con, sf)

    #print("DuckDB Views:")
    #print(con.sql("SELECT view_name FROM duckdb_views();").df().to_string())


    #Parquet test
    con.sql("COPY sf1lineitem TO '/data/test/sf1lineitem.parquet' (FORMAT PARQUET);")

    # DuckDB overview
    #con.sql("SELECT * FROM information_schema.schemata").show()
    con.sql("SELECT * FROM information_schema.tables").show()
    #con.sql("SELECT * FROM information_schema.columns").show(max_rows=100000, max_col_with=100000)


    # Get Substrait queries
    substrait_queries = []  # list[SubstraitQuery]

    print('\n\tProducing Substrait Queries:\n\n')
    for sf in os.listdir("/data"):
        if sf.startswith("sf"):
            for q in os.listdir("/queries/tpch_sql"):
                s_q = prodSubstraitQuery(sf, q)
                if isinstance(s_q, SubstraitQuery):
                    substrait_queries.append(s_q)
                else:
                    con = s_q


    # Check SubstraitQuery Objects
    #print("Results Substrait queries:")
    #for s in substrait_queries:
    #    print(s.__str__())

    # Save Substrait Queries as json
    #for it in substrait_queries:
    #    with open('/data/substrait/json/substrait_queries.json', 'a+') as f:
    #        json.dump(it.__dict__(), f)

    print("\n\n\tConsuming with DuckDB:\n\n")

    # Test DuckDB Engine with different queries & different sf
    results = []    # list[TestResult]

    print('Run query tests..\n')
    for query in substrait_queries:
        t_r = testDuckDB(query)
        if t_r is not None:
            results.append(t_r)

    print("\nResults DuckDB:\n")
    for r in results:
        print(r.__str__())

    # Plot
    #plotResults(results, sf_plot)

    #os.system("conda run -n test-db python tests.py")

    # |-------|
    # | Acero |
    # |-------|

    print("\n\tConsuming with Acero:\n\n")

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

                        reader = substrait.run_query(
                            substrait_bytes, table_provider=table_provider
                        )
                        result = reader.read_all().to_pandas()
                        print(f"Acero result on {p_sf.split('_')[1].upper()}:")
                        print(result)
                    except Exception as e:
                        print(f"ACERO Exception with {p_sf.split('_')[1].upper()} on {p_sf.split('_')[0]}:")
                        print(f" Error: {repr(e)}\n")

    with open('data/temp/substrait_queries.json', 'r') as f:
        sq_data = json.load(f)
    print("SQ Data")
    print(sq_data)

    # |------------|
    # | Datafusion |
    # |------------|

    print("\n\tConsuming with Datafusion:\n\n")

    # Get datafusion context obj
    ctx = getSessionContextDF()

    #  Get query-protobuf and execute
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
                print(f"DATAFUSION Exception with {pf.split('_')[1].upper()} on {pf.split('_')[0]}:")
                print(f" Error: {repr(e)}\n")




# graphviz
# grafisch


# tpch_generator
# sudo make build
# docker volume create test-data
# docker run -it --rm -v test-data:/data --name tpch_sql-generator tpch_sql-generator

# run tests
# sudo make build
# sudo docker run -it --rm --name=test --mount source=test-data,destination=/data benchmark_test

'''