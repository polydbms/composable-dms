from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List
from src.compodb import CompoDB as CDB
from tests.benchmark import Benchmark as BM
from tests.benchmark_result import BenchmarkResult
from tests.substrait_visualizer import SubstraitVisualizer
import logging
import os
import json

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

class QueryData(BaseModel):
    query_name: str
    parser_name: str
    query_plan: str


@router.post("/new-compodb")
async def new_compodb(compodb: CompoDB):
    try:
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
        print(f"CompoDB - Parser: {compoDB.parser}, Optimizer: {compoDB.optimizer}, Execution Engine: {compoDB.executionEngine}")
    print(f"Queries: {', '.join(benchmark.queries)}")

    return {"message": "Benchmark ran successfully", "data": results_json}


@router.post("/visualize-substrait")
async def visualize_substrait(query_data: List[QueryData]):
    """API endpoint to generate multiple DAG images."""
    results = []
    try:
        for query in query_data:
            filename = SubstraitVisualizer.generate_dag_image(query.query_name, query.parser_name, json.loads(query.query_plan))
            results.append({
                "query_name": query.query_name,
                "parser_name": query.parser_name,
                "image_url": f"/static/{filename}"
            })
        return JSONResponse(content={"images": results})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@router.post("/clear")
async def clear():
    CDB.clear_instances()
    return {"message": "CompoDBs cleared successfully"}
