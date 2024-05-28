SELECT l_orderkey,
       l_partkey
FROM lineitem
GROUP BY l_orderkey,
         l_partkey;