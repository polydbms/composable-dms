### Benchmark Suite

The integrated Benchmark Suite is centralized in the `class Benchmark` which can be found at `compodb-core/tests`.
It contains all the logic and functionality to initialize and run the individual benchmarks and what they encompass. 
At its core, it initializes everything at startup and then provides a method to run the benchmarks which are called from 
the `/run-benchmark` route. 
```python  
def run_benchmark(cls, queries: List[str], input_format) -> List[BenchmarkResult]:
```

There, the organization is handled of parsing the queries into substrait plans, optimizing those and executing them on 
the data provided in the database context.

Benchmarks available include the TPC-H Benchmark, the TPC-DS Benchmark, the Join Order Benchmark and the StackOverflow 
Benchmark.
