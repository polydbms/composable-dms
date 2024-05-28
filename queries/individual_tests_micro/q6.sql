SELECT FROM
    lineitem
WHERE l_quantity >= 1
    AND l_quantity <= 1 + 10
    AND l_shipmode IN ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON')