import os
import polars as pl
import duckdb
#import time
#import re
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

from substrait_producer import duckdb_producer, ibis_producer, isthmus_producer, datafusion_producer
from substrait_consumer import duckdb_engine, datafusion_engine, acero_engine
import plotter as plt


def get_sql_query(q, qs):
    with open(f'/queries/{qs}/{q}') as file:
        query = file.read()
    return query

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

    # CSV data
    #data_path = Path(__file__).parent / "data" / "tpch_csv"
    #data_path.mkdir(parents=True, exist_ok=True)
    #lock_file = data_path / "data.json"
    #with FileLock(str(lock_file) + ".lock"):
    #    con = duckdb.connect()
    #    con.execute(f"CALL dbgen(sf={scale_factor})")
    #    con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT CSV);")
    #    con.close()

    # PARQUET data
    data_path = Path(__file__).parent / "data" / "tpch_parquet"
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf={scale_factor})")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")
        con.close()

def create_csv_results(results, sf, query_set):
    df = pl.DataFrame()
    filename = f"tpch_measurements_sf{sf}_{query_set.split('_')[2]}.csv"
    if query_set.split('_')[2] != "original":
        df = df.with_columns((pl.Series(["DuckDB", "Ibis", "DataFusion", "Isthmus"])).alias("Producer"))
    else:
        df = df.with_columns((pl.Series(["DuckDB", "Ibis", "DataFusion", "Isthmus", "SQL"])).alias("Producer"))

    for i in range(1, 23):
        duckdb_col = []
        datafusion_col = []
        acero_col = []
        for r in results:
            if (r.q[1:] == str(i)) and (r.producer == 'DuckDB'):
                if r.engine == 'DuckDB':
                    duckdb_col.append(r.runtime)
                elif r.engine == 'DataFusion':
                    datafusion_col.append(r.runtime)
                elif r.engine == 'Acero':
                    acero_col.append(r.runtime)
        if len(duckdb_col) == 0: duckdb_col.append(0)
        if len(datafusion_col) == 0: datafusion_col.append(0)
        if len(acero_col) == 0: acero_col.append(0)

        for r in results:
            if (r.q[1:] == str(i)) and (r.producer == 'Ibis'):
                if r.engine == 'DuckDB':
                    duckdb_col.append(r.runtime)
                elif r.engine == 'DataFusion':
                    datafusion_col.append(r.runtime)
                elif r.engine == 'Acero':
                    acero_col.append(r.runtime)
        if len(duckdb_col) == 1: duckdb_col.append(0)
        if len(datafusion_col) == 1: datafusion_col.append(0)
        if len(acero_col) == 1: acero_col.append(0)

        for r in results:
            if (r.q[1:] == str(i)) and (r.producer == 'DataFusion'):
                if r.engine == 'DuckDB':
                    duckdb_col.append(r.runtime)
                elif r.engine == 'DataFusion':
                    datafusion_col.append(r.runtime)
                elif r.engine == 'Acero':
                    acero_col.append(r.runtime)
        if len(duckdb_col) == 2: duckdb_col.append(0)
        if len(datafusion_col) == 2: datafusion_col.append(0)
        if len(acero_col) == 2: acero_col.append(0)

        for r in results:
            if (r.q[1:] == str(i)) and (r.producer == 'Isthmus'):
                if r.engine == 'DuckDB':
                    duckdb_col.append(r.runtime)
                elif r.engine == 'DataFusion':
                    datafusion_col.append(r.runtime)
                elif r.engine == 'Acero':
                    acero_col.append(r.runtime)
        if len(duckdb_col) == 3: duckdb_col.append(0)
        if len(datafusion_col) == 3: datafusion_col.append(0)
        if len(acero_col) == 3: acero_col.append(0)

        if query_set.split('_')[2] == "original":
            for r in results:
                if (r.q[1:] == str(i)) and (r.producer == '-- SQL'):
                    if r.engine == 'DuckDB':
                        duckdb_col.append(r.runtime)
                    elif r.engine == 'DataFusion':
                        datafusion_col.append(r.runtime)
                    elif r.engine == 'Acero':
                        acero_col.append(r.runtime)
            if len(duckdb_col) == 4: duckdb_col.append(0)
            if len(datafusion_col) == 4: datafusion_col.append(0)
            if len(acero_col) == 4: acero_col.append(0)

        df = df.with_columns((pl.Series(duckdb_col)).alias(f"Q{i}_duckdb"))
        df = df.with_columns((pl.Series(datafusion_col)).alias(f"Q{i}_datafusion"))
        df = df.with_columns((pl.Series(acero_col)).alias(f"Q{i}_acero"))

    df.write_csv(f"/data/{filename}")

    print("Successfully created result csv-file")
    input("Press Enter after you \'docker cp\' the results to the host to exit the container...")

if __name__ == "__main__":
    print("\n\tExecution Engine Benchmark Test\n")

    # Create tables
    sf = input("Enter SF: ")
    create_tpch_data(sf)

    # Init
    results = []    # list[TestResult]
    isthmus_schema_list = get_isthmus_schema()
    query_set = input("Enter query set (tpch_sql_original | tpch_sql_reduced): ")
    #query_set = "tpch_sql_original"
    #query_set = "tpch_sql_reduced"

    # Init Producer
    duckdb_prod = duckdb_producer.DuckDBProducer(sf)
    ibis_prod = ibis_producer.IbisProducer(sf)
    isthmus_prod = isthmus_producer.IsthmusProducer(sf)
    datafusion_prod = datafusion_producer.DataFusionProducer()

    # Init Consumer
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
        datafusion_query = datafusion_prod.produce_substrait(sql_query, q)


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

        if datafusion_query is not None:
            print("\n\nPRODUCER DataFusion:\n")
            duckdb_datafusion_parquet_result = duckdb_cons.test_substrait(datafusion_query, q, sf, 'DataFusion')
            if duckdb_datafusion_parquet_result is not None: results.append(duckdb_datafusion_parquet_result)

            datafusion_datafusion_parquet_result = datafusion_cons.test_substrait(datafusion_query, q, sf, 'DataFusion')
            if datafusion_datafusion_parquet_result is not None: results.append(datafusion_datafusion_parquet_result)

            acero_datafusion_parquet_result = acero_cons.test_substrait(datafusion_query, q, sf, 'DataFusion')
            if acero_datafusion_parquet_result is not None: results.append(acero_datafusion_parquet_result)


        ### SQL
        print("\nSQL\n")
        duckdb_sql_parquet_result = duckdb_cons.test_sql(sql_query, q, sf)
        if duckdb_sql_parquet_result is not None: results.append(duckdb_sql_parquet_result)
        datafusion_sql_parquet_result = datafusion_cons.test_sql(sql_query, q, sf)
        if datafusion_sql_parquet_result is not None: results.append(datafusion_sql_parquet_result)

    # Print results
    count = 0
    for i in range(1,23):
        print("-----------------------------------------------------------------------------------\n\n")
        print(f"Results for Q{i}:")
        print("\n")
        q_count = 0
        for r in results:
            if r.q[1:] == str(i):
                count += 1
                q_count += 1
                print(r.__str__())
        print(f"\n\tCount: {q_count}\n")

    print(f"\nCOUNT: {count}\n")

    # Create csv-Files with Results

    create_csv_results(results, sf, query_set)

    #

'''
    # Save Substrait Queries as json
    #for it in substrait_queries:
    #    with open('/data/substrait/json/substrait_queries.json', 'a+') as f:
    #        json.dump(it.__dict__(), f)


    # Plot
    plotResults(results, sf_plot)

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

# os.system(f"docker cp test:/data/tpch_measurements_sf1_original.csv /home/chris/data/tpch_measurements_sf1_original.csv")

'''