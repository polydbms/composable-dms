from src.compodb import CompoDB
from src.db_context import DBContext
from src.errors import SubstraitError
from tests.benchmark_result import BenchmarkResult
from pathlib import Path
from typing import List
import logging
import os
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

    @classmethod
    def init_tpch(cls, scale_factor = 0.1):
        logger.info("Initializing TPCH database")
        cls.scale_factor = scale_factor

        #create tpch data
        data_path = Path(__file__).parent.parent.parent / "data"
        csv_path = Path(__file__).parent.parent.parent / "data" / "csv"
        parquet_path = Path(__file__).parent.parent.parent / "data" / "parquet"
        data_path.mkdir(parents=True, exist_ok=True)
        csv_path.mkdir(parents=True, exist_ok=True)
        parquet_path.mkdir(parents=True, exist_ok=True)
        lock_file = data_path / "data.json"
        with FileLock(str(lock_file) + ".lock"):
           con = duckdb.connect()
           con.execute(f"CALL dbgen(sf={cls.scale_factor})")
           con.execute(f"EXPORT DATABASE '{csv_path}' (FORMAT CSV);")
           con.execute(f"EXPORT DATABASE '{parquet_path}' (FORMAT PARQUET);")
           con.close()

        DBContext.csv_path = csv_path
        DBContext.parquet_path = parquet_path
        logger.info("Initializing TPCH database done")


    @classmethod
    def run_benchmark(cls, queries: List[str], input_format) -> List[BenchmarkResult]:
        cls.input_format = input_format
        DBContext.input_format = input_format
        cls.results.clear()
        print("RESULTSLIST:")
        print(cls.results)
        # Setup producer & consumers table references
        DBContext.register_tables(input_format)

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
                        logger.info("PROD " + compodb.parser.get_name() +" "+ query_name + ": " + str(e)[:180])  # TODO: remove
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
                        logger.info("EXEC " + compodb.parser.get_name() + " " + compodb.execution_engine.get_name() + " " + benchmark.query_name + ": " + str(e)) # TODO: remove
                        benchmark.add_failure(f"Substrait execution failed for {benchmark.query_name}: {repr(e)}")
                        continue

        return cls.results


    @classmethod
    def register_queries(cls, queries: List[str]) -> None:
        current_file_path = pathlib.Path(__file__).resolve()
        json_query_folder = current_file_path.parent / 'queries' / 'tpch_ibis_json'
        sql_query_folder = current_file_path.parent / 'queries' / 'tpch_sql_original'
        reduced_query_folder = current_file_path.parent / 'queries' / 'tpch_sql_reduced'
        cls.sql_queries = {}
        cls.json_queries = {}
        individual_count = 1
        for query in queries:
            if query.startswith("tpch-q"):
                cls.sql_queries[query.split("-")[1]] = cls.get_tpch_query(f"{sql_query_folder}/{query.split('-')[1]}.sql")
                json_query = cls.get_tpch_query(f"{json_query_folder}/{query.split('-')[1]}.json")
                if json_query is not None: cls.json_queries[query.split("-")[1]] = json_query
            elif query.startswith("reduced"):
                cls.sql_queries[query.split("_")[2]] = cls.get_tpch_query(f"{reduced_query_folder}/{query.split('_')[2]}.sql")
            else:
                cls.sql_queries[f"IQuery-{individual_count}"] = query
                individual_count += 1


    @classmethod
    def get_tpch_query(cls, query_path) -> str:
        try:
            with open(query_path) as file:
                query = file.read()
        except FileNotFoundError:
            query = None
        return query
