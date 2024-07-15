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

select count() from dwh.rule ;
-- where tenant_id = 0;

select count()
from service.rule_del_me; order by rule_id
-- where toDate(dt_load) >= today()-4;

select * from stage.loyalty__crmdata__chequeset_rule_i
where ruleid = 25;

select * from stage.rule where rule_id = 25;