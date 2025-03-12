from src.compodb import CompoDB
from src.db_context import DBContext
from src.errors import SubstraitError
from tests.benchmark_result import BenchmarkResult
from pathlib import Path
from typing import List
import pandas as pd
import logging
import stat
import os
import subprocess
from filelock import FileLock
import duckdb
import pathlib
import json
import time

logger = logging.getLogger()

class Benchmark:

    sql_queries: dict = {}
    json_queries: dict = {}
    scale_factor = None
    input_format = None
    results: List[BenchmarkResult] = []
    data_path = Path(__file__).parent.parent / "data"
    csv_path = Path(__file__).parent.parent / "data" / "csv"
    parquet_path = Path(__file__).parent.parent / "data" / "parquet"
    imdb_path = Path(__file__).parent.parent / "tests" / "data" / "imdb"
    stackoverflow_path = Path(__file__).parent.parent / "tests" / "data" / "stackoverflow"
    benchmark = None


    @classmethod
    def init_imdb(cls):
        logger.info("Initializing imdb data")
        imdb = cls.imdb_path.parent.parent

        if not os.path.isdir(cls.imdb_path):
            logger.info(f"The folder {cls.imdb_path} does not exist. Running download_imdb.sh ...")
            cls.ensure_permissions(imdb / 'download_imdb.sh')
            subprocess.run(['bash', imdb / 'download_imdb.sh'], check=True)
            logger.info(f"Finished running download_imdb.sh")
        elif not os.listdir(cls.imdb_path):  # Folder exists but is empty
            logger.info(f"The folder {cls.imdb_path} exists but is empty. Running download_imdb.sh ...")
            cls.ensure_permissions(imdb / 'download_imdb.sh')
            subprocess.run(['bash', imdb / 'download_imdb.sh'], check=True)
            logger.info(f"Finished running download_imdb.sh")
        else:
            logger.info(f"The folder {cls.imdb_path} exists and is not empty.")

        # duckdb ?
        con = duckdb.connect()
        with open(cls.imdb_path /'schematext.sql', 'r') as file:
            schema_sql = file.read()

        con.execute(schema_sql)

        for filename in os.listdir(cls.imdb_path):
            if filename.endswith('.csv'):
                table_name = filename.replace('.csv', '')
                csv_file = os.path.join(cls.imdb_path, filename)

                # Insert data from the CSV file into the corresponding table
                insert_sql = f"""
                    COPY {table_name} FROM '{csv_file}' (HEADER, DELIMITER ',', FORMAT CSV, IGNORE_ERRORS);
                    """
                con.execute(insert_sql)

        csv_path = cls.csv_path / "imdb"
        parquet_path = cls.parquet_path / "imdb"
        csv_path.mkdir(parents=True, exist_ok=True)
        parquet_path.mkdir(parents=True, exist_ok=True)

        con.execute(f"EXPORT DATABASE '{parquet_path}' (FORMAT PARQUET);")
        con.execute(f"EXPORT DATABASE '{csv_path}' (FORMAT CSV);")

        con.close()
        logger.info("Initializing imdb data done")


    @classmethod
    def init_stackoverflow(cls):
        logger.info("Initializing stackoverflow data")
        stackoverflow = cls.stackoverflow_path.parent.parent

        if not os.path.isdir(cls.stackoverflow_path):
            logger.info(f"The folder {cls.stackoverflow_path} does not exist. Running download_stackoverflow.sh ...")
            cls.ensure_permissions(stackoverflow / 'download_stackoverflow.sh')
            subprocess.run(['bash', stackoverflow / 'download_stackoverflow.sh'], check=True)
            logger.info(f"Finished running download_stackoverflow.sh")
        elif not os.listdir(cls.stackoverflow_path):
            logger.info(f"The folder {cls.stackoverflow_path} exists but is empty. Running download_stackoverflow.sh ...")
            cls.ensure_permissions(stackoverflow / 'download_stackoverflow.sh')
            subprocess.run(['bash', stackoverflow / 'download_stackoverflow.sh'], check=True)
            logger.info(f"Finished running download_stackoverflow.sh")
        else:
            logger.info(f"The folder {cls.stackoverflow_path} exists and is not empty.")

        duckdb_path = cls.data_path / "stackoverflow.duckdb"
        con = duckdb.connect(str(duckdb_path))
        con.execute("PRAGMA memory_limit='20GB';")
        con.execute("PRAGMA threads=12;")

        for filename in os.listdir(cls.stackoverflow_path):
            if filename.endswith('.csv'):
                table_name = filename.replace('.csv', '')
                csv_file = cls.stackoverflow_path / filename
                if con.execute(
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[
                    0] > 0:
                    logger.info(f"Table {table_name} already exists. Skipping creation.")
                else:
                    # Create the table by reading the CSV in chunks and inserting directly
                    logger.info(f"Loading data from {csv_file} into table {table_name}...")

                    # Use the DuckDB CSV reader directly to load data in chunks
                    chunk_size = 100000  # Set the chunk size to fit in memory
                    cursor = con.cursor()
                    cursor.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}', header=True, all_varchar=True, sample_size=10000, ignore_errors=True);")

                    # Insert data from the chunk in increments (if you need finer chunking)
                    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
                        chunk_file = f"/tmp/{table_name}_chunk.csv"
                        chunk.to_csv(chunk_file, index=False)  # Save chunk to a temporary CSV file
                        cursor.execute(
                            f"INSERT INTO {table_name} SELECT * FROM read_csv_auto('{chunk_file}', header=True, all_varchar=True, ignore_errors=True);")

                    logger.info(f"Table {table_name} created and data inserted.")

        csv_path = cls.csv_path / "stackoverflow"
        parquet_path = cls.parquet_path / "stackoverflow"
        csv_path.mkdir(parents=True, exist_ok=True)
        parquet_path.mkdir(parents=True, exist_ok=True)

        for table in con.execute("SHOW TABLES").fetchall():
            table_name = table[0]
            con.execute(f"COPY {table_name} TO '{parquet_path}/{table_name}.parquet' (FORMAT PARQUET);")
            con.execute(f"COPY {table_name} TO '{csv_path}/{table_name}.csv' (FORMAT CSV);")

        con.close()

        if duckdb_path.exists():
            duckdb_path.unlink()

        logger.info("Initializing stackoverflow data done")

    @classmethod
    def init_tpcds(cls, scale_factor = 0.1):
        logger.info("Initializing TPC-DS data")

        csv_path = cls.csv_path / "tpcds"
        parquet_path = cls.parquet_path / "tpcds"
        csv_path.mkdir(parents=True, exist_ok=True)
        parquet_path.mkdir(parents=True, exist_ok=True)

        lock_file = cls.data_path / "data.json"
        with FileLock(str(lock_file) + ".lock"):
           con = duckdb.connect()
           con.execute(f"CALL dsdgen(sf={cls.scale_factor})")
           con.execute(f"EXPORT DATABASE '{csv_path}' (FORMAT CSV);")
           con.execute(f"EXPORT DATABASE '{parquet_path}' (FORMAT PARQUET);")
           con.close()

        logger.info("Initializing TPC-DS data done")

    @classmethod
    def init_tpch(cls, scale_factor = 0.1):
        logger.info("Initializing TPC-H data")
        cls.scale_factor = scale_factor
        cls.data_path.mkdir(parents=True, exist_ok=True)
        cls.csv_path.mkdir(parents=True, exist_ok=True)
        cls.parquet_path.mkdir(parents=True, exist_ok=True)

        csv_path = cls.csv_path / "tpch"
        parquet_path = cls.parquet_path / "tpch"
        csv_path.mkdir(parents=True, exist_ok=True)
        parquet_path.mkdir(parents=True, exist_ok=True)

        lock_file = cls.data_path / "data.json"
        with FileLock(str(lock_file) + ".lock"):
           con = duckdb.connect()
           con.execute(f"CALL dbgen(sf={cls.scale_factor})")
           con.execute(f"EXPORT DATABASE '{csv_path}' (FORMAT CSV);")
           con.execute(f"EXPORT DATABASE '{parquet_path}' (FORMAT PARQUET);")
           con.close()

        DBContext.csv_path = "app" / cls.csv_path
        DBContext.parquet_path = "app" / cls.parquet_path
        logger.info("Initializing TPCH data done")


    @classmethod
    def run_benchmark(cls, queries: List[str], input_format) -> List[BenchmarkResult]:
        cls.results.clear()

        if len(queries) == 0:
            return []
        if queries[0].startswith('tpch'):
            cls.benchmark = 'tpch'
        elif queries[0].startswith('tpcds'):
            cls.benchmark = 'tpcds'
        elif queries[0].startswith('j-o-b'):
            cls.benchmark = 'imdb'
        else:
            return []

        # Setup producer & consumers table references
        DBContext.register_tables(input_format, cls.benchmark)

        # Setup queries
        cls.register_queries(queries)

        # Run Benchmark
        for compodb in CompoDB.get_compodb_instances():
            # Parse to Substrait Plan
            if compodb.parser.get_name() == "Ibis": # TODO Ibis ??
                for query_name, query in cls.json_queries.items():
                    benchmark_result = BenchmarkResult(compodb.parser.get_name(), compodb.get_optimizer_names(),
                                                       compodb.execution_engine.get_name(), input_format, cls.scale_factor)
                    substrait_plan = None
                    try:
                        substrait_plan = compodb.parser.to_substrait(query)
                        benchmark_result.query_name = query_name
                        benchmark_result.substrait_query = substrait_plan
                        cls.results.append(benchmark_result)
                    except SubstraitError as e:
                        logger.info("PROD " + compodb.parser.get_name() +" "+ query_name + ": " + str(e)[:180]) # TODO: remove
                        benchmark_result.query_name = query_name
                        benchmark_result.add_failure(f"Substrait production failed for {query_name}: {repr(e)}")
                        cls.results.append(benchmark_result)
                        continue
            else:
                for query_name, query in cls.sql_queries.items():
                    benchmark_result = BenchmarkResult(compodb.parser.get_name(), compodb.get_optimizer_names(),
                                                       compodb.execution_engine.get_name(), input_format, cls.scale_factor)
                    substrait_plan = None
                    try:
                        substrait_plan = compodb.parser.to_substrait(query)
                        benchmark_result.query_name = query_name
                        benchmark_result.substrait_query = substrait_plan
                        cls.results.append(benchmark_result)
                    except SubstraitError as e:
                        logger.info("PROD " + compodb.parser.get_name() +" "+ query_name + ": " + str(e)[:150])
                        benchmark_result.query_name = query_name
                        benchmark_result.add_failure(f"Substrait production failed for {query_name}: {repr(e)}")
                        cls.results.append(benchmark_result)
                        continue

            for benchmark in cls.results:
                if benchmark.parser_name == "Calcite" and benchmark.execution_engine_name == "DataFusion" and benchmark.query_name == "q17":
                    benchmark.error_msg = "PanicException: Q17 not supported"
                if benchmark.error_msg is None:

                    # Optimize Substrait Plan
                    if compodb.optimizer:
                        for opt in compodb.optimizer:
                            benchmark.substrait_query = opt.optimize_substrait(benchmark.substrait_query)

                    # Run Substrait & Measure
                    times = []
                    query_result = None
                    try:
                        for i in range(4):
                            stCPU = time.process_time()
                            query_result = compodb.execution_engine.run_substrait(benchmark.substrait_query)
                            etCPU = time.process_time()
                            resCPU = (etCPU - stCPU) * 1000
                            if  (i == 1) | (i == 2) | (i == 3):
                                times.append(resCPU)
                        timeAVG = (times[0] + times[1] + times[2]) / 3
                        benchmark.measurements = times
                        #benchmark.query_result = query_result
                        benchmark.runtime = timeAVG
                    except SubstraitError as e:
                        logger.info("EXEC " + compodb.parser.get_name() + " " + compodb.execution_engine.get_name() + " " + benchmark.query_name + ": " + str(e)[:150])
                        benchmark.add_failure(f"Substrait execution failed for {benchmark.query_name}: {repr(e)}")
                        continue

        return cls.results


    @classmethod
    def register_queries(cls, queries: List[str]) -> None:
        current_file_path = pathlib.Path(__file__).resolve()
        json_query_folder = current_file_path.parent / 'queries' / 'tpch_ibis_json'
        sql_query_folder = current_file_path.parent / 'queries' / 'tpch_sql_original'
        reduced_query_folder = current_file_path.parent / 'queries' / 'tpch_sql_reduced'
        job_query_folder = current_file_path.parent / 'queries' / 'join-order-benchmark'
        tpcds_query_folder = current_file_path.parent / 'queries' / 'tpcds'
        cls.sql_queries = {}
        cls.json_queries = {}
        individual_count = 1
        for query in queries:
            if query.startswith("tpch-q"):
                cls.sql_queries[query.split("-")[1]] = cls.get_query(f"{sql_query_folder}/{query.split('-')[1]}.sql")
                json_query = cls.get_query(f"{json_query_folder}/{query.split('-')[1]}.json")
                if json_query is not None: cls.json_queries[query.split("-")[1]] = json_query
            elif query.startswith("reduced"):
                cls.sql_queries[query.split("_")[2]] = cls.get_query(f"{reduced_query_folder}/{query.split('_')[2]}.sql")
            elif query.startswith("j-o-b"):
                cls.sql_queries[query.split("_")[1]] = cls.get_query(f"{job_query_folder}/{query.split('_')[1]}.sql")
            elif query.startswith("tpcds"):
                cls.sql_queries[query.split("-")[1]] = cls.get_query(f"{tpcds_query_folder}/{query.split('-')[1]}.sql")
            else:
                cls.sql_queries[f"IQuery-{individual_count}"] = query
                individual_count += 1


    @classmethod
    def get_query(cls, query_path) -> str:
        try:
            with open(query_path) as file:
                query = file.read()
        except FileNotFoundError:
            query = None
        return query

    @classmethod
    def ensure_permissions(cls, script_path):
        if not os.access(script_path, os.X_OK):
            os.chmod(script_path, stat.S_IRWXU)
            print(f"Permissions updated for {script_path} to be executable.")
        else:
            print(f"{script_path} already has execute permission.")