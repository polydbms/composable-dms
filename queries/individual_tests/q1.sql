SELECT
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price
FROM
    lineitem