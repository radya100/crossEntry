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
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and (lower(query) like '%pcvalue_daily%' or lower(query) like '%pcvalue_retro%')
order by event_time;

show create stage.v_pcvalue_insert_sliding_window;

select tenant_id, count() from dwh.pcvalue_retro group by tenant_id;

select * from stage.pcvalue where tenant_id = 1;

select * from system.tables where create_table_query like '%stage.pcvalue%';

show create null.mv_to_pcvalue_from_null_pcvalue;

drop table `null`.mv_to_pcvalue_from_null_pcvalue on cluster basic;
CREATE MATERIALIZED VIEW `null`.mv_to_pcvalue_from_null_pcvalue on cluster basic TO stage.pcvalue AS
SELECT
    get_instance(tenant_id) AS instance_id,
    tenant_id,
    cityHash64(instance_id, tenant_id, assumeNotNull(external_id), assumeNotNull(contact_id), assumeNotNull(card_id), assumeNotNull(product_iconurl), assumeNotNull(value), assumeNotNull(effectivefrom), assumeNotNull(effectiveto), assumeNotNull(message), assumeNotNull(description), assumeNotNull(condition_email), assumeNotNull(discount), assumeNotNull(red_pice), assumeNotNull(black_pice), assumeNotNull(product_name_for_conclusion), assumeNotNull(priority), assumeNotNull(group_id)) AS hash_sum,
    external_id,
    contact_id,
    card_id,
    product_iconurl,
    value,
    effectivefrom,
    effectiveto,
    message,
    description,
    condition_email,
    discount,
    red_pice,
    black_pice,
    product_name_for_conclusion AS product_name,
    priority,
    group_id
FROM `null`.null_pcvalue
where tenant_id <> 10;
