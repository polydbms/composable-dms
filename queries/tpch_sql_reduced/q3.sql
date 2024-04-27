SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    o_orderdate
LIMIT 10;