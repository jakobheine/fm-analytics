create table market_values
(
    id              serial primary key          not null,
    player_id       text                        not null,
    market_value    bigint                      not null,
    valid_from      timestamp with time zone    not null,
    valid_to        timestamp with time zone    null
);