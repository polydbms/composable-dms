# Benchmarking Framework for composable DMS

This is a Python Framework for benchmarking composable data management systems.

The default tests are run using the widely known TPC-H Benchmark, but the testing can be extended by adding additional 
query sets. The tests are organized in such a way that when executed, a prompt asks for the scaling factors to be tested 
and DuckDB then generates the data. After that, the TPC-H queries get translated to Substrait query plans 
through different Substrait Producers and then transferred to a number of execution engines that consume and run these 
Substrait plans. The execution times of each execution engine are measured precisely and made available through a csv 
export.

## Setup

The Benchmark is executed in a docker container. For that, you first have to create the volume for our framework. To 
create the volume, run:

```commandline
docker volume create test-data
```

All the install requirements for the python framework are set up automatically at runtime.

## How to run tests

Available query sets can be viewed in the `queries` folder. The subfolders represent the query sets that contain the 
queries used for the benchmark. The TPC-H queries can be found there. Additional queries to be tested can be added by 
adding a new query set folder.

To conduct a test run, you first have to build the docker image from the Dockerfile by running:

```commandline
make build
```

When the image is created, you can start the Benchmark Test by:

```commandline
docker run -it --rm --name=test --mount source=test-data,destination=/data benchmark_test
```

Right after starting the test, you will get prompted to enter the desired Scale Factors for the tests. You can enter 
multiple numbers representing the TPC-H Scale Factors (SFs) in the way that SF 1 equals a dataset volume of 1GB. 
If multiple numbers are entered, they need to be separated by a space like `1 10` for tests on SF 1 and on SF 10.
After all the tests and measurements are done, you get prompted to export the results by opening a second Shell and 
running:

```commandline
docker cp test:/data/results/ [destination-path on local machine]
```