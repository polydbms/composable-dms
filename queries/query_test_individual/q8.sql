SELECT FIRST_VALUE(l_linenumber) OVER (
        ORDER BY l_extendedprice ASC
    ) AS LeastExpensive
FROM lineitem