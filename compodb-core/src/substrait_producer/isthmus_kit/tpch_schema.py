import pyarrow as pa

lineitem_schema = pa.schema([
    ('l_orderkey', pa.int64()),
    ('l_partkey', pa.int64()),
    ('l_suppkey', pa.int64()),
    ('l_linenumber', pa.int64()),
    ('l_quantity', pa.decimal128(15, 2)),
    ('l_extendedprice', pa.decimal128(15, 2)),
    ('l_discount', pa.decimal128(15, 2)),
    ('l_tax', pa.decimal128(15, 2)),
    ('l_returnflag', pa.string()),
    ('l_linestatus', pa.string()),
    ('l_shipdate', pa.date32()),
    ('l_commitdate', pa.date32()),
    ('l_receiptdate', pa.date32()),
    ('l_shipinstruct', pa.string()),
    ('l_shipmode', pa.string()),
    ('l_comment', pa.string())
])

customer_schema = pa.schema([
    ('c_custkey', pa.int64()),
    ('c_name', pa.string()),
    ('c_address', pa.string()),
    ('c_nationkey', pa.int64()),
    ('c_phone', pa.string()),
    ('c_acctbal', pa.decimal128(19, 0)),
    ('c_mktsegment', pa.string()),
    ('c_comment', pa.string())
])

nation_schema = pa.schema([
    ('n_nationkey', pa.int64()),
    ('n_name', pa.string()),
    ('n_regionkey', pa.int64()),
    ('n_comment', pa.string())
])

orders_schema = pa.schema([
    ('o_orderkey', pa.int64()),
    ('o_custkey', pa.int64()),
    ('o_orderstatus', pa.string()),
    ('o_totalprice', pa.decimal128(19, 0)),
    ('o_orderdate', pa.date32()),
    ('o_orderpriority', pa.string()),
    ('o_clerk', pa.string()),
    ('o_shippriority', pa.int32()),
    ('o_comment', pa.string())
])

part_schema = pa.schema([
    ('p_partkey', pa.int64()),
    ('p_name', pa.string()),
    ('p_mfgr', pa.string()),
    ('p_brand', pa.string()),
    ('p_type', pa.string()),
    ('p_size', pa.int32()),
    ('p_container', pa.string()),
    ('p_retailprice', pa.decimal128(19, 0)),
    ('p_comment', pa.string())
])

partsupp_schema = pa.schema([
    ('ps_partkey', pa.int64()),
    ('ps_suppkey', pa.int64()),
    ('ps_availqty', pa.int32()),
    ('ps_supplycost', pa.decimal128(19, 0)),
    ('ps_comment', pa.string())
])

region_schema = pa.schema([
    ('r_regionkey', pa.int64()),
    ('r_name', pa.string()),
    ('r_comment', pa.string())
])

supplier_schema = pa.schema([
    ('s_suppkey', pa.int64()),
    ('s_name', pa.string()),
    ('s_address', pa.string()),
    ('s_nationkey', pa.int64()),
    ('s_phone', pa.string()),
    ('s_acctbal', pa.decimal128(19, 0)),
    ('s_comment', pa.string())
])