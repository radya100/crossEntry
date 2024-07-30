--===== Логи загрузки =========

select
    dt_load
    , count()
    , uniqExact((rule_id, tenant_id))
from dwh.rule
group by dt_load with rollup
order by dt_load desc;

select event_time
     , intDiv(query_duration_ms, 60000) as min
     , formatReadableQuantity(written_rows), query
from system.query_log
where event_date >= today()-1
    and type = 'QueryFinish'
    and query like 'insert into dwh.rule%'
    and written_rows > 0
order by event_time desc;


select *
from dwh.rule
where instance_id = 7
    and rule_id = 380;

select dt_load, rule_name, is_active, is_del
from stage.rule
where instance_id = 7
    and rule_id = 380
order by dt_load;
