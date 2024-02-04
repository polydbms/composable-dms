import os
import pandas as pd
import duckdb

q1 = ("select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, "
      "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,) sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
      "avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order "
      "from sf1.lineitem where l_shipdate <= date '1998-12-01' - interval '90' day (3) group by l_returnflag, l_linestatus order by l_returnflag, "
      "l_linestatus;")

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
    con.execute(query=f'CREATE SCHEMA sf{sf}')

    for t in os.listdir(f'/data/sf{sf}'):
        if t.split(".")[1]=="csv":
            con.sql(f'CREATE TABLE sf{sf}.{t.split(".")[0]} AS FROM read_csv("/data/sf{sf}/{t}");')


if __name__ == "__main__":
    print("Hello World")
    print(os.listdir("/data"))

    # Connect to new DuckDB instance & load substrait extension
    con = duckdb.connect('duck.db')
    con.install_extension("substrait")
    con.load_extension("substrait")

    # Create csv-Tables & ingest into DuckDB-db
    for f in os.listdir("/data"):
        formatTables(str(f).split('f')[1])
        insertDuckDB(con, str(f).split('f')[1])

    con.execute(query="CREATE TABLE crossfit (exercise TEXT, difficulty_level INT)")
    con.execute(
        query="INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , ('Push Jerk', 7), ('Bar Muscle Up', 10)")

    proto_bytes = con.get_substrait(query="SELECT count(L_RETURNFLAG) AS items FROM sf1.lineitem").fetchone()[0]

    query_result = con.from_substrait(proto=proto_bytes)
    print(query_result)
    #DuckDB overview
    con.sql("SELECT * FROM information_schema.schemata").show()
    con.sql("SELECT * FROM information_schema.tables").show()
    con.sql("SELECT * FROM information_schema.columns").show(max_rows=100000, max_col_with=100000)




    #con.sql("Select * from sf1.lineitem").show()

    #proto_bytes = con.get_substrait(query="SELECT count(*) as test FROM sf1.lineitem").fetchone()[0]
    #query_result = con.from_substrait(proto=proto_bytes)

    #json = con.get_substrait_json("Select * from sf1.lineitem").fetchone()[0]
    #print(json)



    #print(q1)
    #con.sql(str(q1)).show()




    #proto_bytes = \
    #    con.get_substrait(
    #        query="SELECT count(exercise) AS exercise FROM crossfit WHERE difficulty_level <= 5").fetchone()[0]

    #query_result = con.from_substrait(proto=proto_bytes)
    #print(query_result)
