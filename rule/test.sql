select *
from stage.rule_keys
-- where is_del = 0
limit 1 by source_table;

select * from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and query like '%rule%'
order by event_time desc