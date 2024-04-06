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
from substrait_consumer import duckdb_engine, datafusion_engine, acero_engine


def get_sql_query(q, qs):
    with open(f'/queries/{qs}/{q}') as file:
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
    #print(isthmus_schema)
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
    query_set = "tpch_sql_backup"

    # Init Producers
    duckdb_prod = duckdb_producer.DuckDBProducer(sf)
    ibis_prod = ibis_producer.IbisProducer(sf)
    isthmus_prod = isthmus_producer.IsthmusProducer(sf)

    # Init Tester
    duckdb_cons = duckdb_engine.DuckDBConsumer()
    datafusion_cons = datafusion_engine.DataFusionConsumer()
    datafusion_isthmus_cons = datafusion_engine.DataFusionConsumer('Isthmus')
    acero_cons = acero_engine.AceroConsumer()


    # SQL
    for q in os.listdir(f"/queries/{query_set}"):

        print("\n--------------------------------------------------------------------------")
        print(f"\n\t{q.split('.')[0].upper()}:\n")

        sql_query = get_sql_query(q, query_set)
        duckdb_query = duckdb_prod.produce_substrait(sql_query, q)
        ibis_query = ibis_prod.produce_substrait(q)
        isthmus_query = isthmus_prod.produce_substrait(isthmus_schema_list, sql_query)
        # Format: consumer_producer_format_result
        if duckdb_query is not None:
            print("\n\nPRODUCER DuckDB:\n")
            duckdb_duckdb_parquet_result = duckdb_cons.test_substrait(duckdb_query, q, sf, 'DuckDB')
            if duckdb_duckdb_parquet_result is not None: results.append(duckdb_duckdb_parquet_result)

            datafusion_duckdb_parquet_result = datafusion_cons.test_substrait(duckdb_query, q, sf, 'DuckDB')
            if datafusion_duckdb_parquet_result is not None: results.append(datafusion_duckdb_parquet_result)

            acero_duckdb_parquet_result = acero_cons.test_substrait(duckdb_query, q, sf, 'DuckDB')
            if acero_duckdb_parquet_result is not None: results.append(acero_duckdb_parquet_result)

        if ibis_query is not None:
            print("\n\nPRODUCER Ibis:\n")
            duckdb_ibis_parquet_result = duckdb_cons.test_substrait(ibis_query, q, sf, 'Ibis')
            if duckdb_ibis_parquet_result is not None: results.append(duckdb_ibis_parquet_result)

            datafusion_ibis_parquet_result = datafusion_cons.test_substrait(ibis_query, q, sf, 'Ibis')
            if datafusion_ibis_parquet_result is not None: results.append(datafusion_ibis_parquet_result)

            acero_ibis_parquet_result = acero_cons.test_substrait(ibis_query, q, sf, 'Ibis')
            if acero_ibis_parquet_result is not None: results.append(acero_ibis_parquet_result)

        if isthmus_query is not None:
            print("\n\nPRODUCER Isthmus:\n")
            duckdb_isthmus_parquet_result = duckdb_cons.test_substrait(isthmus_query, q, sf, 'Isthmus')
            if duckdb_isthmus_parquet_result is not None: results.append(duckdb_isthmus_parquet_result)

            datafusion_isthmus_parquet_result = datafusion_isthmus_cons.test_substrait(isthmus_query, q, sf, 'Isthmus')
            if datafusion_isthmus_parquet_result is not None: results.append(datafusion_isthmus_parquet_result)

            acero_isthmus_parquet_result = acero_cons.test_substrait(isthmus_query, q, sf, 'Isthmus')
            if acero_isthmus_parquet_result is not None: results.append(acero_isthmus_parquet_result)
        ### SQL
        duckdb_sql_parquet_result = duckdb_cons.test_sql(sql_query, q, sf)
        if duckdb_sql_parquet_result is not None: results.append(duckdb_sql_parquet_result)

        datafusion_sql_parquet_result = datafusion_cons.test_sql(sql_query, q, sf)
        if datafusion_sql_parquet_result is not None: results.append(datafusion_sql_parquet_result)

    # Print results
    print("\n\n")
    for r in results:
        print(r.__str__())


'''
    # Save Substrait Queries as json
    #for it in substrait_queries:
    #    with open('/data/substrait/json/substrait_queries.json', 'a+') as f:
    #        json.dump(it.__dict__(), f)


    # Plot
    #plotResults(results, sf_plot)

    #os.system("conda run -n test-db python tests.py")

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