CREATE TABLE store_returns
(
    sr_returned_date_sk       BIGINT,
    sr_return_time_sk         BIGINT,
    sr_item_sk                BIGINT NOT NULL,
    sr_customer_sk            BIGINT,
    sr_cdemo_sk               BIGINT,
    sr_hdemo_sk               BIGINT,
    sr_addr_sk                BIGINT,
    sr_store_sk               BIGINT,
    sr_reason_sk              BIGINT,
    sr_ticket_number          BIGINT NOT NULL,
    sr_return_quantity        BIGINT,
    sr_return_amt             DECIMAL(15, 2),
    sr_return_tax             DECIMAL(15, 2),
    sr_return_amt_inc_tax     DECIMAL(15, 2),
    sr_fee                    DECIMAL(15, 2),
    sr_return_ship_cost       DECIMAL(15, 2),
    sr_refunded_cash          DECIMAL(15, 2),
    sr_reversed_charge        DECIMAL(15, 2),
    sr_store_credit           DECIMAL(15, 2),
    sr_net_loss               DECIMAL(15, 2)
);