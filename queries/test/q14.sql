SELECT
    sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice
        ELSE
            0
        END) / sum(l_extendedprice) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= date '1995-09-01'
