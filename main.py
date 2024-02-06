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

def formatTables(sf):
    # Iterate tabular TPC-H Schema
    for t in os.listdir(f'/data/sf{sf}'):
        # Add column names
        match t:
            case "orders.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['O_ORDERKEY', 'O_CUSTKEY',
                        'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK','O_SHIPPRIORITY', 'O_COMMENT'])
            case "nation.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['N_NATIONKEY', 'N_NAME',
                        'N_REGIONKEY', 'N_COMMENT'])
            case "lineitem.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['L_ORDERKEY', 'L_PARTKEY',
                        'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG',
                        'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])
            case "region.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['R_REGIONKEY', 'R_NAME', 'R_COMMENT'])
            case "supplier.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['S_SUPPKEY', 'S_NAME', 'S_ADDRESS',
                        'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'])
            case "partsupp.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['PS_PARTKEY', 'PS_SUPPKEY',
                        'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'])
            case "part.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['P_PARTKEY', 'P_NAME', 'P_MFGR',
                        'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'])
            case "customer.tbl":
                df = pd.read_table(f'/data/sf{sf}/{t}', delimiter='|', names=['C_CUSTKEY', 'C_NAME', 'C_ADDRESS',
                        'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'])
            case _:
                continue
        # Safe as csv-File
        df.to_csv(f'/data/sf{sf}/{t.split(".")[0]}.csv', index=False)

def insertDuckDB(con, sf):
    for t in os.listdir(f'/data/sf{sf}'):
        if t.split(".")[1]=="csv":
            con.sql(f'CREATE TABLE sf{sf}{t.split(".")[0]} AS FROM read_csv("/data/sf{sf}/{t}");')

def testDuckDB(query):
    stWT = time.time()
    stCPU = time.process_time()
    query_result = con.from_substrait(proto=query.proto)
    etCPU = time.process_time()
    etWT = time.time()
    resWT = (etWT - stWT) * 1000
    resCPU = (etCPU - stCPU) * 1000
    print(f'\nQuery {query.q} on {query.sf}:\n')
    print(query_result)
    resTuple = (query.sf, query.q, resWT, resCPU)

    return resTuple

def getSubstraitQuery(sf, q):
    with open(f'/queries/{q}') as file:
        match q:
            case "q1.txt":
                reg = re.compile(r"\bLINEITEM.*?\b")
                rep = sf + "lineitem"
                subquery = re.sub(reg, rep, file.read(), 1)
    return SubstraitQuery(sf, q.split('.')[0], con.get_substrait(query=subquery).fetchone()[0])

def plotResults(resultsDuckDB):
    fig, ax = plt.subplots()
    sfx = []
    y = []
    for result in resultsDuckDB:
        sfx.append(result[0])
        y.append(result[3])
    ax.bar(sfx, y)
    ax.set_ylabel('CPU runtime in ms')
    ax.set_title('TPC-H Q1 on DuckDB')
    plt.savefig("/data/plot.png")

if __name__ == "__main__":
    print("Hello World")
    print(os.listdir("/data"))
    print(os.listdir("/queries"))

    # Remove sf
    #shutil.rmtree("/data/sf10")
    #print(os.listdir("/data"))


    # Connect to new DuckDB instance & load substrait extension
    con = duckdb.connect('duck.db')
    con.install_extension("substrait")
    con.load_extension("substrait")


    # Create csv-Tables & ingest into DuckDB-db
    for f in os.listdir("/data"):
        formatTables(str(f).split('f')[1])
        insertDuckDB(con, str(f).split('f')[1])


    # DuckDB overview
    #con.sql("SELECT * FROM information_schema.schemata").show()
    #con.sql("SELECT * FROM information_schema.tables").show()
    #con.sql("SELECT * FROM information_schema.columns").show(max_rows=100000, max_col_with=100000)


    # Get Substrait queries
    substrait_queries = []  # list[SubstraitQuery]

    for sf in os.listdir("/data"):
        for q in os.listdir("/queries"):
            substrait_queries.append(getSubstraitQuery(sf, q))


    # Check SubstraitQuery Objects
    #print("Results Substrait queries:")
    #for s in substrait_queries:
    #    print(s.__str__())


    # Test DuckDB Engine with different queries & different sf.
    resultsDuckDB = []      # Tuple: (sf#, q#, [duration_millisecondsWT], [duration_millisecondsCPU])

    for query in substrait_queries:
        con.sql(f"SELECT COUNT(L_RETURNFLAG) FROM {query.sf}lineitem;").show()
        resultsDuckDB.append(testDuckDB(query))

    print("Results DuckDB:")
    print(resultsDuckDB)

    # Plot
    plotResults(resultsDuckDB)

