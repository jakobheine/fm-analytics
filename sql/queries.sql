select m.number, p.first_name, p.last_name, sum(score) as points_matchday from events
inner join players p on p.id = events.player_id
inner join matchdays m on events.matchday_id = m.id
group by m.number, p.first_name, p.last_name

select avg(sub.points_matchday) from
    (select m.number, p.first_name, p.last_name, sum(score) as points_matchday from events
inner join players p on p.id = events.player_id
inner join matchdays m on events.matchday_id = m.id
group by m.number, p.first_name, p.last_name) sub

select avg(sub.points_matchday) from
    (select m.number, p.first_name, p.last_name, sum(score) as points_matchday from events
inner join players p on p.id = events.player_id
inner join matchdays m on events.matchday_id = m.id
group by m.number, p.first_name, p.last_name) sub
group by number

