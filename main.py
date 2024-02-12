import os
import pandas as pd
import duckdb
import time
import re
import matplotlib.pyplot as plt
import numpy as np
import json
#from datafusion import SessionContext
#from datafusion import substrait as ss
import sys
import shutil


class SubstraitQuery:
    def __init__(self, sf, q, proto):
        self.sf = sf
        self.q = q
        self.proto = proto

    def __str__(self):
        return f"SF: {self.sf}, Q: {self.q}, PROTO: {self.proto}"

    def __dict__(self):
        return f"{{'sf':'{self.sf}', 'q':'{self.q}', 'proto':'{self.proto}'}}"
    

class TestResult:
    def __init__(self, benchmark, engine, sf, q, query_result, measurements, runtime):
        self.benchmark = benchmark
        self.engine = engine
        self.sf = sf
        self.q = q
        self.query_result = query_result
        self.measurements = measurements
        self.runtime = runtime

    def __str__(self):
        return (f"Test Result of {self.benchmark} query {self.q} on {self.engine} with {self.sf}:\n"
                f" Measurements:\t{self.measurements}\n Runtime: \t{self.runtime} ms\n")

def formatTables(sf):
    # Iterate tabular TPC-H Schema
    for t in os.listdir(f'/data/{sf}'):
        # Add column names
        match t:
            case "orders.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['O_ORDERKEY', 'O_CUSTKEY',
                        'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK','O_SHIPPRIORITY', 'O_COMMENT'])
            case "nation.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['N_NATIONKEY', 'N_NAME',
                        'N_REGIONKEY', 'N_COMMENT'])
            case "lineitem.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['L_ORDERKEY', 'L_PARTKEY',
                        'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG',
                        'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])
            case "region.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])
            case "supplier.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS',
                        'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
            case "partsupp.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['PS_PARTKEY', 'PS_SUPPKEY',
                        'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])
            case "part.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['P_PARTKEY', 'P_NAME', 'P_MFGR',
                        'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
            case "customer.tbl":
                df = pd.read_table(f'/data/{sf}/{t}', delimiter='|', names=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS',
                        'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])
            case _:
                continue
        # Safe as csv-File
        df.to_csv(f'/data/{sf}/{t.split(".")[0]}.csv', index=False)


def insertDuckDB(con, sf):
    for t in os.listdir(f'/data/{sf}'):
        if t.split(".")[1]=="csv":
            con.sql(f'CREATE TABLE {sf}{t.split(".")[0]} AS FROM read_csv("/data/{sf}/{t}");')


def testDuckDB(query):
    #global query_result
    #print(f'\nQuery {query.q} on {query.sf}:\n')
    times = []
    try:
        for i in range(4):
            stCPU = time.process_time()
            query_result = con.from_substrait(proto=query.proto)
            etCPU = time.process_time()
            resCPU = (etCPU - stCPU) * 1000
            if (i == 1) | (i == 2) | (i == 3):
                times.append(resCPU)
        timeAVG = (times[0]+times[1]+times[2])/3
        #print(query_result)
        res_obj = TestResult('TPC-H', 'DuckDB', query.sf, query.q, query_result, times, timeAVG)
        return res_obj
    except Exception as e:
        print(f"EXCEPTION: from_substrait() not working on{query.q}, {query.sf}: {repr(e)}")
        #print(query.proto)
        return None


def getSubstraitQuery(sf, q):
    # Define sf substitution param
    reg = re.compile(r"\$")

    # Substitute table names with corresponding sf-table names
    with open(f'/queries/{q}') as file:
        subquery = re.sub(reg, sf, re.sub(reg, sf, re.sub(reg, sf, re.sub(reg, sf, re.sub(reg, sf, re.sub(reg,
                    sf, re.sub(reg, sf, re.sub(reg, sf, file.read()))))))))

    #print("SUBQUERY:")
    #print(subquery)
    #con.sql("PRAGMA disable_optimizer;")
    #con.sql(subquery).show()

    # Test
    sql_t = con.get_substrait(query="SELECT * FROM lineitem").fetchone()[0]
    with open(f"/data/substrait/proto/test_q.proto", "wb") as f:
        f.write(sql_t)


    # Get the Substrait protobuf-Query, else restart duckdb and continue
    try:
        sq_obj = SubstraitQuery(sf, q.split('.')[0], con.get_substrait(query=subquery).fetchone()[0])
        with open(f"/data/substrait/proto/{sf}_{q.split('.')[0]}_substrait.proto", "wb") as f:
            f.write(sq_obj.proto)
        return sq_obj
    except Exception as e:
        print(f"EXCEPTION: get_substrait() not working on {q.split('.')[0]}, {sf}: {repr(e)}")
        #print(f"SUBQUERY: {subquery}")
        recon_ddb = restoreDuckDB()
        return recon_ddb


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


def connectDuckDB():
    print('Connecting to DuckDB..')
    con_ddb = duckdb.connect("data/duck.db")
    con_ddb.install_extension("substrait")
    con_ddb.load_extension("substrait")
    print(' done\n')
    return con_ddb


def restoreDuckDB():
    print(' restore duckdb..')
    con.close()
    recon = duckdb.connect("data/duck.db")
    recon.install_extension("substrait")
    recon.load_extension("substrait")
    return recon

def addToData(con, sf):
    print(f'Creating and inserting {sf}-tables..')
    formatTables(sf)
    insertDuckDB(con, sf)
    print(' --> done')


if __name__ == "__main__":
    print("\n\tExecution Engine Benchmark Test\n")



    # Create csv-Tables & ingest into DuckDB-db, if not yet there


    # Connect to DuckDB instance & load substrait extension
    con = connectDuckDB()

    sf_plot = {}
    print('Checking if data already exists..')
    data_added = False
    tab_rel = con.sql("SELECT table_name FROM duckdb_tables();").df()
    for f in os.listdir("/data"):
        if f.startswith("sf"):                   # Temp
            sf_plot.update({f: ()})
            sfbool = False
            for t in tab_rel['table_name']:
                if t.startswith(f):
                    sfbool = True
            if not sfbool:
                print(f" --> {f} data missing\n")
                data_added = True
                addToData(con, f)
    if not data_added:
        print(' --> data found, skipping data ingestion\n')

    con.sql(f'CREATE TABLE lineitem AS FROM read_csv("/data/sf1/lineitem.csv");')

    # DuckDB overview
    #con.sql("SELECT * FROM information_schema.schemata").show()
    #con.sql("SELECT * FROM information_schema.tables").show()
    #con.sql("SELECT * FROM information_schema.columns").show(max_rows=100000, max_col_with=100000)


    # Get Substrait queries
    substrait_queries = []  # list[SubstraitQuery]

    print('\n\tProduce Substrait Queries..\n\n')
    for sf in os.listdir("/data"):
        if sf.startswith("sf"):
            for q in os.listdir("/queries"):
                s_q = getSubstraitQuery(sf, q)
                if isinstance(s_q, SubstraitQuery):
                    substrait_queries.append(s_q)
                else:
                    con = s_q


    # Check SubstraitQuery Objects
    #print("Results Substrait queries:")
    #for s in substrait_queries:
    #    print(s.__str__())

    # Save Substrait Queries as json
    #for it in substrait_queries:
    #    with open('/data/substrait/json/substrait_queries.json', 'a+') as f:
    #        json.dump(it.__dict__(), f)

    print("\n\n\tConsuming with DuckDB:\n\n")

    # Test DuckDB Engine with different queries & different sf
    results = []    # list[TestResult]

    print('Run query tests..\n')
    for query in substrait_queries:
        t_r = testDuckDB(query)
        if t_r is not None:
            results.append(t_r)

    print("\nResults DuckDB:\n")
    for r in results:
        print(r.__str__())

    # Plot
    plotResults(results, sf_plot)

    os.system("conda run -n test-db python tests.py")






# graphviz
# grafisch


# tpch_generator
# sudo make build
# docker volume create test-data
# docker run -it --rm -v test-data:/data --name tpch-generator tpch-generator

# run tests
# sudo make build
# sudo docker run -it --rm --name=test --mount source=test-data,destination=/data benchmark_test