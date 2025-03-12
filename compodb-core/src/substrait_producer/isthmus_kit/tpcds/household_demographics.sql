CREATE TABLE household_demographics
(
    hd_demo_sk                BIGINT NOT NULL,
    hd_income_band_sk         BIGINT,
    hd_buy_potential          VARCHAR,
    hd_dep_count              BIGINT,
    hd_vehicle_count          BIGINT
);