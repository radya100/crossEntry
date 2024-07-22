select *
from system.columns
where database = 'dwh'
    and table = 'pcvalue_retro'
    and default_kind not in ('MATERIALIZED', 'ALIAS')
order by position;

select *
from system.columns
where database = 'stage'
    and table = 'pcvalue_ce'
    and default_kind not in ('MATERIALIZED', 'ALIAS')
order by position;

select *  from stage.pcvalue_ce;

select * from system.tables where create_table_query like '%stage.pcvalue%';

select *
from system.query_log
where event_date = today()
--     and user = 'airflow_user'
    and type <> 'QueryStart'
    and lower(query) like '%pcvalue%';

show create stage.v_pcvalue_insert_sliding_window;