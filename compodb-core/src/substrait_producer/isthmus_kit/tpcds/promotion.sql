CREATE TABLE promotion
(
    p_promo_sk                BIGINT NOT NULL,
    p_promo_id                VARCHAR NOT NULL,
    p_start_date_sk           BIGINT,
    p_end_date_sk             BIGINT,
    p_item_sk                 BIGINT,
    p_cost                    DECIMAL(15, 2),
    p_response_target         BIGINT,
    p_promo_name              VARCHAR,
    p_channel_dmail           VARCHAR,
    p_channel_email           VARCHAR,
    p_channel_catalog         VARCHAR,
    p_channel_tv              VARCHAR,
    p_channel_radio           VARCHAR,
    p_channel_press           VARCHAR,
    p_channel_event           VARCHAR,
    p_channel_demo            VARCHAR,
    p_channel_details         VARCHAR,
    p_purpose                 VARCHAR,
    p_discount_active         VARCHAR
);