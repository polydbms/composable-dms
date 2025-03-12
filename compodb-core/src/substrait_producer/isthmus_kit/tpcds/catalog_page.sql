CREATE TABLE catalog_page
(
    cp_catalog_page_sk        BIGINT NOT NULL,
    cp_catalog_page_id        VARCHAR NOT NULL,
    cp_start_date_sk          BIGINT,
    cp_end_date_sk            BIGINT,
    cp_department             VARCHAR,
    cp_catalog_number         BIGINT,
    cp_catalog_page_number    BIGINT,
    cp_description            VARCHAR,
    cp_type                   VARCHAR
);