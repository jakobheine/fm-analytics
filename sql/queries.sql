select m.number, p.first_name, p.last_name, sum(score) as points_matchday from events
inner join players p on p.id = events.player_id
inner join matchdays m on events.matchday_id = m.id
group by m.number, p.first_name, p.last_name;

select avg(sub.points_matchday) from
    (select m.number, p.first_name, p.last_name, sum(score) as points_matchday from events
inner join players p on p.id = events.player_id
inner join matchdays m on events.matchday_id = m.id
group by m.number, p.first_name, p.last_name) sub;

select avg(sub.points_matchday) from
    (select m.number, p.first_name, p.last_name, sum(score) as points_matchday from events
inner join players p on p.id = events.player_id
inner join matchdays m on events.matchday_id = m.id
group by m.number, p.first_name, p.last_name) sub
group by number;

select max(sub.index) as latest_crawled_matchday from
    (select m.index from matchdays m
inner join events e on m.id = e.matchday_id
group by m.index) sub;

select id from market_values
        where player_id='3fe2aac5-ac32-5053-9c02-5669b3e9c296'
        and market_value=50000000
        and valid_to is null;

insert into market_values (player_id, market_value, valid_from)
    VALUES (player_id, market_value, now());

UPDATE market_values SET valid_to = now() WHERE id = ?;

