SELECT
    count(l_returnflag) AS num_returnflag,
    count(l_discount) AS num_discount
FROM
    lineitem