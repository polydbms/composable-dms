CREATE TABLE call_center
(
    cc_call_center_sk         BIGINT NOT NULL,
    cc_call_center_id         VARCHAR NOT NULL,
    cc_rec_start_date         DATE,
    cc_rec_end_date           DATE,
    cc_closed_date_sk         BIGINT,
    cc_open_date_sk           BIGINT,
    cc_name                   VARCHAR,
    cc_class                  VARCHAR,
    cc_employees              BIGINT,
    cc_sq_ft                  BIGINT,
    cc_hours                  VARCHAR,
    cc_manager                VARCHAR,
    cc_mkt_id                 BIGINT,
    cc_mkt_class              VARCHAR,
    cc_mkt_desc               VARCHAR,
    cc_market_manager         VARCHAR,
    cc_division               BIGINT,
    cc_division_name          VARCHAR,
    cc_company                BIGINT,
    cc_company_name           VARCHAR,
    cc_street_number          VARCHAR,
    cc_street_name            VARCHAR,
    cc_street_type            VARCHAR,
    cc_suite_number           VARCHAR,
    cc_city                   VARCHAR,
    cc_county                 VARCHAR,
    cc_state                  VARCHAR,
    cc_zip                    VARCHAR,
    cc_country                VARCHAR,
    cc_gmt_offset             DECIMAL(15, 2),
    cc_tax_percentage         DECIMAL(15, 2)
);