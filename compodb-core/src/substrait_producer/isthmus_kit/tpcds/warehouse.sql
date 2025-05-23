CREATE TABLE warehouse (
    w_warehouse_sk            BIGINT NOT NULL,
    w_warehouse_id            VARCHAR NOT NULL,
    w_warehouse_name          VARCHAR,
    w_warehouse_sq_ft         BIGINT,
    w_street_number           VARCHAR,
    w_street_name             VARCHAR,
    w_street_type             VARCHAR,
    w_suite_number            VARCHAR,
    w_city                    VARCHAR,
    w_county                  VARCHAR,
    w_state                   VARCHAR,
    w_zip                     VARCHAR,
    w_country                 VARCHAR,
    w_gmt_offset              DECIMAL(15,2)
);