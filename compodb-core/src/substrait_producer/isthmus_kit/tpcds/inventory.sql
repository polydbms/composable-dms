CREATE TABLE inventory
(
    inv_date_sk               BIGINT NOT NULL,
    inv_item_sk               BIGINT NOT NULL,
    inv_warehouse_sk          BIGINT NOT NULL,
    inv_quantity_on_hand      BIGINT
);