from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import List
from src.compodb import CompoDB as CDB
from tests.benchmark import Benchmark as BM
from tests.benchmark_result import BenchmarkResult
import logging

router = APIRouter()

logger = logging.getLogger()

class CompoDB(BaseModel):
    parser: str
    optimizer: List[str]
    executionEngine: str

class Benchmark(BaseModel):
    compoDBs: List[CompoDB]
    queries: List[str]
    inputFormat: str


@router.post("/new-compodb")
async def new_compodb(compodb: CompoDB):
    try:
        print("COMPODB")
        print(compodb.dict())
        CDB(compodb.parser, compodb.optimizer, compodb.executionEngine)
        logger.info(f"New compodb: {compodb}")
    except Exception as e:
        raise e

    return {"message": "CompoDB added successfully", "data": compodb.dict()}


@router.post("/run-benchmark")
async def run_benchmark(benchmark: Benchmark):

    results = BM.run_benchmark(benchmark.queries, benchmark.inputFormat)

    results_json = [result.json() for result in results]

    for compoDB in benchmark.compoDBs:
        print("CompoDB:")
        print(f"Optimizer: {compoDB.optimizer}")
        print(f"Execution Engine: {compoDB.executionEngine}")
    print("Tested with:")
    print(f"Queries: {', '.join(benchmark.queries)}")
    print(f"Input Format: {benchmark.inputFormat}")

    return {"message": "Benchmark ran successfully", "data": results_json}


@router.post("/clear")
async def clear():
    CDB.clear_instances()
    return {"message": "CompoDBs cleared successfully"}