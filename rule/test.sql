--===== Логи загрузки =========

select
    dt_load
    , count()
from dwh.rule
group by dt_load
order by dt_load desc;

select event_time
     , intDiv(query_duration_ms, 60000) as min
     , formatReadableQuantity(written_rows), query
from system.query_log
where event_date >= today()-1
    and type = 'QueryFinish'
    and query like 'insert into dwh.rule%'
--     and written_rows > 0
order by event_time desc;

