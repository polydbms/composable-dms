CREATE TABLE partsupp (
  PS_PARTKEY BIGINT NOT NULL,
  PS_SUPPKEY BIGINT NOT NULL,
  PS_AVAILQTY INTEGER,
  PS_SUPPLYCOST DECIMAL,
  PS_COMMENT VARCHAR(199)
);