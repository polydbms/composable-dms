SELECT
    o_orderpriority
FROM
    orders
WHERE
    o_orderdate >= date '1993-07-01'
    AND o_orderdate < date '1993-10-01'
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;