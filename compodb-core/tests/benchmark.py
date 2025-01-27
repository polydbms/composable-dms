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
        cls.results.clear()
        # Setup producer & consumers table references
        DBContext.register_tables(input_format)

        # Setup queries
        cls.register_queries(queries)

        # Run Benchmark
        for compodb in CompoDB.get_compodb_instances():
            if compodb.producer.get_name() == "Ibis":
                for query_name, query in cls.json_queries.items():
                    benchmark_result = BenchmarkResult(compodb.producer.get_name(), compodb.consumer.get_name(),
                                                       input_format, cls.scale_factor, query_name)

                    # Produce Substrait Plan
                    substrait_plan = None
                    try:
                        substrait_plan = compodb.producer.produce_substrait(query)
                        benchmark_result.substrait_query = substrait_plan
                    except SubstraitError as e:
                        print(e[0:100]) # TODO: remove
                        benchmark_result.add_failure(f"Substrait production failed for {query_name}: {repr(e)}")
                        cls.results.append(benchmark_result)
                        continue

                    # Run Substrait & Measure
                    times = []
                    query_result = None
                    try:
                        for i in range(4):
                            stCPU = time.process_time()
                            query_result = compodb.consumer.run_substrait(substrait_plan)
                            etCPU = time.process_time()
                            resCPU = (etCPU - stCPU) * 1000
                            if  (i == 1) | (i == 2) | (i == 3):
                                times.append(resCPU)
                        timeAVG = (times[0] + times[1] + times[2]) / 3
                        benchmark_result.measurements = times
                        #benchmark_result.query_result = query_result
                        benchmark_result.runtime = timeAVG
                    except SubstraitError as e:
                        print(e[0:100]) # TODO: remove
                        benchmark_result.add_failure(f"Substrait execution failed for {query_name}: {repr(e)}")
                        cls.results.append(benchmark_result)
                        continue

                    cls.results.append(benchmark_result)
            else:
                for query_name, query in cls.sql_queries.items():
                    benchmark_result = BenchmarkResult(compodb.producer.get_name(), compodb.consumer.get_name(),
                                                       input_format, cls.scale_factor, query_name)

                    # Produce Substrait Plan
                    substrait_plan = None
                    try:
                        substrait_plan = compodb.producer.produce_substrait(query)
                        benchmark_result.substrait_query = substrait_plan
                    except SubstraitError as e:
                        print(e)  # TODO: remove
                        benchmark_result.add_failure(f"Substrait production failed for {query_name}: {repr(e)}")
                        cls.results.append(benchmark_result)
                        continue

                    # Run Substrait & Benchmark
                    times = []
                    query_result = None
                    try:
                        for i in range(4):
                            stCPU = time.process_time()
                            query_result = compodb.consumer.run_substrait(substrait_plan)
                            etCPU = time.process_time()
                            resCPU = (etCPU - stCPU) * 1000
                            if (i == 1) | (i == 2) | (i == 3):
                                times.append(resCPU)
                        timeAVG = (times[0] + times[1] + times[2]) / 3
                        benchmark_result.measurements = times
                        #benchmark_result.query_result = query_result
                        benchmark_result.runtime = timeAVG
                    except SubstraitError as e:
                        print(e)  # TODO: remove
                        benchmark_result.add_failure(f"Substrait execution failed for {query_name}: {repr(e)}")
                        cls.results.append(benchmark_result)
                        continue

                    cls.results.append(benchmark_result)

        # DBContext.deregister_tables() # TODO: Needed ?

        return cls.results


    @classmethod
    def register_queries(cls, queries: List[str]) -> None:
        current_file_path = pathlib.Path(__file__).resolve()
        json_query_folder = current_file_path.parent / 'queries' / 'tpch_ibis_json'
        sql_query_folder = current_file_path.parent / 'queries' / 'tpch_sql_original'
        cls.sql_queries = {}
        cls.json_queries = {}
        individual_count = 1
        for query in queries:
            if query.startswith("tpch-q"):
                cls.sql_queries[query.split("-")[1]] = cls.get_tpch_query(f"{sql_query_folder}/{query.split('-')[1]}.sql")
                json_query = cls.get_tpch_query(f"{json_query_folder}/{query.split('-')[1]}.json")
                if json_query is not None: cls.json_queries[query.split("-")[1]] = json_query
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
