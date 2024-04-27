SELECT
    sum(l_extendedprice) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= date '1994-01-01'
    AND l_shipdate < date '1995-01-01'
    AND l_discount > 0.05
    AND l_discount < 0.07
    AND l_quantity < 24;