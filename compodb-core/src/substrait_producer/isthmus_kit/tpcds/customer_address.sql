CREATE TABLE customer_address (
    ca_address_sk BIGINT NOT NULL,
    ca_address_id VARCHAR NOT NULL,
    ca_street_number VARCHAR,
    ca_street_name VARCHAR,
    ca_street_type VARCHAR,
    ca_suite_number VARCHAR,
    ca_city VARCHAR,
    ca_county VARCHAR,
    ca_state VARCHAR,
    ca_zip VARCHAR,
    ca_country VARCHAR,
    ca_gmt_offset DECIMAL(15, 2),
    ca_location_type VARCHAR
);