--===== Логи загрузки =========

select
    dt_load
    , count()
from service.campaign_del_me
group by dt_load
order by dt_load desc;

select event_time
     , intDiv(query_duration_ms, 60000) as min
     , formatReadableQuantity(written_rows), query
from system.query_log
where event_date >= today()-1
    and type = 'QueryFinish'
    and query like 'insert into service.campaign_del_me%'
--     and written_rows > 0
order by event_time desc;

truncate table stage.campaign_log on cluster basic;
truncate table service.campaign_del_me on cluster basic;