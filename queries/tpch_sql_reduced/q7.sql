SELECT
    l_year
FROM (
    SELECT
        extract(year FROM l_shipdate) AS l_year
    FROM
        supplier,
        lineitem,
        orders,
        customer
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND l_shipdate BETWEEN date '1995-01-01'
        AND date '1996-12-31') AS shipping
GROUP BY
    l_year
ORDER BY
    l_year;