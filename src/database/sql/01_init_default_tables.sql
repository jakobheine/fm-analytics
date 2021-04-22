create table market_values
(
    id              serial primary key          not null,
    player_id       text                        not null,
    market_value    bigint                      not null,
    valid_from      timestamp with time zone    not null,
    valid_to        timestamp with time zone    null
);

create table events
(
    index                   bigint                          not null,      
    additional_minutes      bigint                          not null,
    corrected               boolean                         not null,
    datetime                timestamp with time zone        not null,
    id                      bigint                          not null,             
    minutes                 bigint                          not null,
    score                   bigint                          not null,              
    type                    text                            not null,
    matchday_id             text                            not null, 
    player_id               text                            not null
);
