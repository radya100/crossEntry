select *
from stage.rule_keys
-- where is_del = 0
limit 1 by source_table;

select * from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and query like '%rule%'
order by event_time desc;



select * from service.rule_del_me where tenant_id = 0;

select event_time
     , intDiv(query_duration_ms, 60000) as min
     , formatReadableQuantity(written_rows), query
from system.query_log
where event_date >= today()-1
    and type = 'QueryFinish'
    and query like 'insert into service.rule_del_me%'
order by event_time desc;

select * from dwh.rule
where tenant_id = 0;