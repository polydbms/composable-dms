SELECT
    nation,
    sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        l_extendedprice AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey) AS profit
GROUP BY
    nation
ORDER BY
    nation