CREATE TABLE time_dim (
    t_time_sk                 BIGINT NOT NULL,
    t_time_id                 VARCHAR NOT NULL,
    t_time                    BIGINT,
    t_hour                    BIGINT,
    t_minute                  BIGINT,
    t_second                  BIGINT,
    t_am_pm                   VARCHAR,
    t_shift                   VARCHAR,
    t_sub_shift               VARCHAR,
    t_meal_time               VARCHAR
);