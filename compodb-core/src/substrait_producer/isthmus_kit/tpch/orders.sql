CREATE TABLE orders (
  O_ORDERKEY BIGINT NOT NULL,
  O_CUSTKEY BIGINT NOT NULL,
  O_ORDERSTATUS CHAR(1),
  O_TOTALPRICE DECIMAL,
  O_ORDERDATE DATE,
  O_ORDERPRIORITY CHAR(15),
  O_CLERK CHAR(15),
  O_SHIPPRIORITY INTEGER,
  O_COMMENT VARCHAR(79)
);