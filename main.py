import os
import polars as pl
import duckdb
import yaml
from datetime import datetime

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
from compo_db import CompoDB



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

    queries = len(os.listdir(f"/queries/{query_set}"))+1
    print(f"Queries in {query_set}: {str(queries)}")

    for i in range(1, queries):
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

    df.write_csv(f"/data/results/{filename}")

    print(f"Successfully created result csv-file for the {query_set.split('_')[2]} query set on sf{sf}")

def export_benchmark_result(result: TestResult, filename) -> None:
    # TestResult as row
    df = pl.DataFrame({"ScaleFactor": f"{result.sf}", "QuerySet": f"{result.query_set}", "Query": f"{result.query_name}",
                       "PlanProducer": f"{result.producer}", "Engine": f"{result.engine}",
                       "Measurements": f"{result.times.measurements}", "Runtime": f"{result.times.runtime}"})
    if not export_file:
        df.write_csv(filename, separator=",")
    else:
        try:
            with open(filename, mode="ab") as f:
                df.write_csv(f, has_header=False) #Check
        except Exception as e:
            print(f"Exception while writing to the Export File: {repr(e)}")

    return None



def load_compodb_config(config_file):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
        compiler_conf = config["compiler"]
        optimizer_conf = config["optimizer"]
        engine_conf = config["engine"]
    return compiler_conf, optimizer_conf, engine_conf


if __name__ == "__main__":

    print("\n\tWelcome to CompoDB\n")

    # Configuration of CompoDB

    print("> Read in configuration ..\n")
    compiler, optimizer, engines = load_compodb_config("configuration.yml")
    print("> CompoDB configured as follows:\n>\n>   Compiler:")
    for comp in compiler:
        print(f">\t{comp}")
    print(">   Optimizer:")
    for opt in optimizer:
        print(f">\t{opt}")
    print(">   Engines:")
    for eng in engines:
        print(f">\t{eng}")

    # Create CompoDB

    compodb = CompoDB(compiler, optimizer, engines)
    print("\n> CompoDB built successfully!\n")

    # Get Scale Factors for Benchmark data

    sf_input = input("\n> Enter Scale Factor(s): ")
    sf_arr = sf_input.split(" ")
    print(f"> {sf_arr}")

    # Run the Benchmark

    print("\n\n\tStarting the Benchmark\n")

    # Init
    isthmus_schema_list = get_isthmus_schema()
    results = []    # list[TestResult]

    export_filename = f"/benchmark_results/benchmark_results.csv"
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    if os.path.isfile(export_filename):
        os.system(f"mv {export_filename} {export_filename.split('.'[0])}_{str(now)}.csv")
    export_file = False
    queries_created = False
    substrait_queries = {}


    for sf in sf_arr:
        print(f"Creating {sf}GB of testing data ...")
        create_tpch_data(sf)
        print(" data successfully created")

        for query_set in ["tpch_sql_original", "tpch_sql_reduced"]:
            print(f"Starting the Benchmark for the {query_set} query set ...")

            #query_set = input("Enter query set (tpch_sql_original | tpch_sql_reduced): ")

            # Query testing
            for q in os.listdir(f"/queries/{query_set}"):

                print("\n--------------------------------------------------------------------------")
                print(f"\n\tRUN {query_set.split('_')[2]} {q.split('.')[0].upper()}:\n")

                sql_query = get_sql_query(q, query_set)

                # Create all the Substrait queries once

                if not queries_created:
                    if compodb.duckdb_opt:
                        duckdb_query = compodb.duckdb_optimizer.produce_substrait(sql_query, q, query_set)
                        if duckdb_query is not None:
                            substrait_queries.update({"DuckDB": {f"{query_set.split('_')[2]}": {f"{q.split('.')[0]}": duckdb_query}}})
                    if compodb.ibis_comp:
                        ibis_query = compodb.ibis_compiler.produce_substrait(q, query_set)
                        if ibis_query is not None:
                            substrait_queries.update({"Ibis": {f"{query_set.split('_')[2]}": {f"{q.split('.')[0]}": ibis_query}}})
                    if compodb.datafusion_opt:
                        datafusion_query = compodb.datafusion_optimizer.produce_substrait(sql_query, q, query_set)
                        if datafusion_query is not None:
                            substrait_queries.update({"DataFusion": {f"{query_set.split('_')[2]}": {f"{q.split('.')[0]}": datafusion_query}}})
                    if compodb.calcite_opt:
                        calcite_query = compodb.calcite_optimizer.produce_substrait(isthmus_schema_list, sql_query, q, query_set)
                        if calcite_query is not None:
                            substrait_queries.update({"Calcite": {f"{query_set.split('_')[2]}": {f"{q.split('.')[0]}": calcite_query}}})

                # Run the queries
                # Execute on DuckDBs Engine

                if compodb.duckdb_eng:

                    if compodb.duckdb_opt:
                        try:
                            query_result, benchmark_times = compodb.duckdb_engine.substrait(
                                substrait_queries["DuckDB"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("DuckDB", "DuckDB",
                                                     f'{substrait_queries["DuckDB"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                     f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                     f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.datafusion_opt:
                        try:
                            query_result, benchmark_times = compodb.duckdb_engine.substrait(
                                substrait_queries["DataFusion"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("DataFusion", "DuckDB",
                                                     f'{substrait_queries["DataFusion"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                     f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                     f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.calcite_opt:
                        try:
                            query_result, benchmark_times = compodb.duckdb_engine.substrait(
                                substrait_queries["Calcite"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("Calcite", "DuckDB",
                                                     f'{substrait_queries["Calcite"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                     f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                     f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.ibis_comp:
                        try:
                            query_result, benchmark_times = compodb.duckdb_engine.substrait(
                                substrait_queries["Ibis"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("Ibis", "DuckDB",
                                                     f'{substrait_queries["Ibis"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                     f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                     f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    # SQL test
                    query_result, benchmark_times = compodb.duckdb_engine.sql(sql_query)
                    if query_result is not None:
                        result = TestResult("SQL", "DuckDB",
                                            f'{sql_query}',
                                            f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                            f"{sf}", 'Parquet', query_result, benchmark_times)
                        export_benchmark_result(result, export_filename)
                        export_file = True
                        results.append(result)

                # Execute on DataFusions Engine

                if compodb.datafusion_eng:

                    if (query_set.split('_')[2] == 'original') and (q == 'q18.sql' or q == 'q1.sql' or q == 'q6.sql'):  # DataFusion thread panics at index out of bounds
                        continue

                    if compodb.duckdb_opt:
                        try:
                            query_result, benchmark_times = compodb.datafusion_engine.substrait(
                                substrait_queries["DuckDB"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("DuckDB", "DataFusion",
                                                    f'{substrait_queries["DuckDB"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.datafusion_opt:
                        try:
                            query_result, benchmark_times = compodb.datafusion_engine.substrait(
                                substrait_queries["DataFusion"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("DataFusion", "DataFusion",
                                                    f'{substrait_queries["DataFusion"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.calcite_opt:
                        try:
                            query_result, benchmark_times = compodb.datafusion_engine.substrait(
                                substrait_queries["Calcite"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"], 'Isthmus')
                            if query_result is not None:
                                result = TestResult("Calcite", "DataFusion",
                                                    f'{substrait_queries["Calcite"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.ibis_comp:
                        try:
                            query_result, benchmark_times = compodb.datafusion_engine.substrait(
                                substrait_queries["Ibis"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("Ibis", "DataFusion",
                                                    f'{substrait_queries["Ibis"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    # SQL test
                    query_result, benchmark_times = compodb.datafusion_engine.sql(sql_query)
                    if query_result is not None:
                        result = TestResult("SQL", "DataFusion",
                                            f'{sql_query}',
                                            f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                            f"{sf}", 'Parquet', query_result, benchmark_times)
                        export_benchmark_result(result, export_filename)
                        export_file = True
                        results.append(result)

                # Execute on DataFusions Engine

                if compodb.acero_eng:

                    if compodb.duckdb_opt:
                        try:
                            query_result, benchmark_times = compodb.acero_engine.substrait(
                                substrait_queries["DuckDB"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("DuckDB", "Acero",
                                                    f'{substrait_queries["DuckDB"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.datafusion_opt:
                        try:
                            query_result, benchmark_times = compodb.acero_engine.substrait(
                                substrait_queries["DataFusion"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("DataFusion", "Acero",
                                                    f'{substrait_queries["DataFusion"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.calcite_opt:
                        try:
                            query_result, benchmark_times = compodb.acero_engine.substrait(
                                substrait_queries["Calcite"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("Calcite", "Acero",
                                                    f'{substrait_queries["Calcite"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

                    if compodb.ibis_comp:
                        try:
                            query_result, benchmark_times = compodb.acero_engine.substrait(
                                substrait_queries["Ibis"][f"{query_set.split('_')[2]}"][f"{q.split('.')[0]}"])
                            if query_result is not None:
                                result = TestResult("Ibis", "Acero",
                                                    f'{substrait_queries["Ibis"][query_set.split("_")[2]][q.split(".")[0]]}',
                                                    f"{query_set.split('_')[2]}", f"{q.split('.')[0]}",
                                                    f"{sf}", 'Parquet', query_result, benchmark_times)
                                export_benchmark_result(result, export_filename)
                                export_file = True
                                results.append(result)
                        except KeyError:
                            pass

        queries_created = True


    print("\n\nBenchmark is completed\n\nThe exported Benchmark results can be found at your local repository in the folder benchmark_results\n\n")


#node --experimental-specifier-resolution=node dist/index.js -p /home/chris1187/BA/substrait-js/substrait_ibis_q1.json -o /home/chris1187/BA/