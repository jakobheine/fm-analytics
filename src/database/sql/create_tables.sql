-- set time zone
SET TIME ZONE 'Europe/Berlin';
-- SET TIME ZONE 'UTC';
SELECT current_timestamp;

create table market_values
(
    id              serial primary key          not null,
    player_id       text                        not null,
    market_value    bigint                      not null,
    valid_from      timestamp with time zone    not null,
    valid_to        timestamp with time zone    null
);

create table players
(
    id                                             text primary key,
    active                                         boolean,
    avg_score                                      bigint,
    club_id                                        text,
    current_match                                  text,
    first_name                                     text,
    first_name_normalized                          text,
    injured                                        boolean,
    jersey_number                                  double precision,
    last_name                                      text,
    last_name_normalized                           text,
    market_value                                   bigint,
    matches_in_starting_lineup                     bigint,
    position                                       text,
    starting_lineup                                boolean,
    suspended                                      boolean,
    total_score                                    bigint,
    trend                                          bigint
);