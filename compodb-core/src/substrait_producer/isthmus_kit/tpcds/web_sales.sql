CREATE TABLE web_sales
(
    ws_sold_date_sk           BIGINT,
    ws_sold_time_sk           BIGINT,
    ws_ship_date_sk           BIGINT,
    ws_item_sk                BIGINT NOT NULL,
    ws_bill_customer_sk       BIGINT,
    ws_bill_cdemo_sk          BIGINT,
    ws_bill_hdemo_sk          BIGINT,
    ws_bill_addr_sk           BIGINT,
    ws_ship_customer_sk       BIGINT,
    ws_ship_cdemo_sk          BIGINT,
    ws_ship_hdemo_sk          BIGINT,
    ws_ship_addr_sk           BIGINT,
    ws_web_page_sk            BIGINT,
    ws_web_site_sk            BIGINT,
    ws_ship_mode_sk           BIGINT,
    ws_warehouse_sk           BIGINT,
    ws_promo_sk               BIGINT,
    ws_order_number           BIGINT NOT NULL,
    ws_quantity               BIGINT,
    ws_wholesale_cost         DECIMAL(15, 2),
    ws_list_price             DECIMAL(15, 2),
    ws_sales_price            DECIMAL(15, 2),
    ws_ext_discount_amt       DECIMAL(15, 2),
    ws_ext_sales_price        DECIMAL(15, 2),
    ws_ext_wholesale_cost     DECIMAL(15, 2),
    ws_ext_list_price         DECIMAL(15, 2),
    ws_ext_tax                DECIMAL(15, 2),
    ws_coupon_amt             DECIMAL(15, 2),
    ws_ext_ship_cost          DECIMAL(15, 2),
    ws_net_paid               DECIMAL(15, 2),
    ws_net_paid_inc_tax       DECIMAL(15, 2),
    ws_net_paid_inc_ship      DECIMAL(15, 2),
    ws_net_paid_inc_ship_tax  DECIMAL(15, 2),
    ws_net_profit             DECIMAL(15, 2)
);