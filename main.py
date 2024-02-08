import os
import pandas as pd
import duckdb
import time
import re
import matplotlib.pyplot as plt
import sys
import shutil


class SubstraitQuery:
    def __init__(self, sf, q, proto):
        self.sf = sf
        self.q = q
        self.proto = proto

    def __str__(self):
        return f"SF: {self.sf}, Q: {self.q}, PROTO: {self.proto}"

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
    global query_result
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
        print(f"\nEXCEPTION: from_substrait() not working on{query.q}, {query.sf}: {repr(e)}")
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

    # Get the Substrait protobuf-Query, else restart duckdb and continue
    try:
        sq_obj = SubstraitQuery(sf, q.split('.')[0], con.get_substrait(query=subquery).fetchone()[0])
        return sq_obj
    except Exception as e:
        print(f"\nEXCEPTION: get_substrait() not working on {q.split('.')[0]}, {sf}: {repr(e)}")
        #print("SUBQUERY:")
        #print(subquery)
        recon_ddb = reconnectDuckDB()
        return recon_ddb


def plotResults(results):
    queries = ()
    sf_plot = {
        'sf1': (),
        'sf2': (),
    }
    for i in range(1, 23):
        if r.q[1:] == str(i):
            queries += (r.q.upper())
            sf_plot.update({'sf1':()})
            # ToDo



    fig, ax = plt.subplots()
    sfx = []
    y = []
    for result in resultsDuckDB:
        sfx.append(result[0])
        y.append(result[3])
    ax.bar(sfx, y)
    ax.set_ylabel('CPU runtime in ms')
    ax.set_title('TPC-H Q1 on DuckDB')
    plt.savefig("/data/results/plots/plot.png")


def connectDuckDB():
    print('Connecting to DuckDB..')
    con_ddb = duckdb.connect("duck.db")
    con_ddb.install_extension("substrait")
    con_ddb.load_extension("substrait")
    print(' done')
    return con_ddb


def reconnectDuckDB():
    print(' Reconnecting to DuckDB..\n')
    con.close()
    recon = duckdb.connect("duck.db")
    recon.install_extension("substrait")
    recon.load_extension("substrait")
    return recon


if __name__ == "__main__":
    print("\n\tExecution Engine Benchmark Test\n")

    # Remove sf
    #shutil.rmtree("/data/sf10")
    #print(os.listdir("/data"))

    # Connect to new DuckDB instance & load substrait extension
    con = connectDuckDB()
    #con.sql("CALL duckdb_databases();").show()


    # Create csv-Tables & ingest into DuckDB-db
    print('Creating and inserting Tables..')
    for f in os.listdir("/data"):
        if f.startswith("sf1") | f.startswith("sf2"):
            formatTables(f)
            insertDuckDB(con, f)
    print(' done')

    # DuckDB overview
    #con.sql("SELECT * FROM information_schema.schemata").show()
    #con.sql("SELECT * FROM information_schema.tables").show()
    #con.sql("SELECT * FROM information_schema.columns").show(max_rows=100000, max_col_with=100000)


    # Get Substrait queries
    substrait_queries = []  # list[SubstraitQuery]

    print('Get Substrait Queries..')
    for sf in os.listdir("/data"):
        if sf.startswith("sf1") | sf.startswith("sf2"):
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


    # Test DuckDB Engine with different queries & different sf
    results = []    # list[TestResult]

    print('\nRun query tests..')
    for query in substrait_queries:
        t_r = testDuckDB(query)
        if t_r is not None:
            results.append(t_r)

    print("\nResults DuckDB:\n")
    for r in results:
        print(r.__str__())

    # Plot
    #plotResults(results)

# graphviz
# grafisch