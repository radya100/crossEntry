select
    name, type
from system.columns
where database = 'stage'
    and table = 'bo'
    and default_kind not in ('MATERIALIZED', 'ALIAS')
    and name not in ('key_hash', 'is_del', 'last_version')
order by position;
--добавил коммент

insert into dwh.bonus_slim_daily
(    b_instance_hash,instance_id,bonus_id,organization_id,source_table,is_delete,value,dt_created,parent_type_id,parent_id,rule_id,campaign_id,is_status,oper_type,chequeitem_id,article_id
    ,ea_ci,cheque_id,shop_id,card_id,card_hash,is_order,credit_bonus_id,dt_start_date,dt_finish_date,remainder,dt_load,cheque_summ,cheque_summdiscounted,tenant_id,ci_quantity,ci_summ,ci_summdiscounted)
select
   cityHash64((arrayJoin(bo) AS bon).7, ci.instance_id) AS b_instance_hash,
   ci.instance_id,
   if( any(bon).6 = 6, any(bon).7, any(bon).18) AS bonus_id,
   any(bon.8) AS organization_id,
   toUInt8(if(any(bon.6) = 6, 1, 2)) AS source_table,
   any(bon.19) AS is_delete,
   any(bon.1) AS value,
   toDateTime(any(bon.10), 'UTC') AS dt_created,
   any(bon.11) AS parent_type_id,
   any(bon.16) AS parent_id,
   any(bon.2) AS rule_id,
   any(bon.3) AS campaign_id,
   any(bon.12) AS is_status,
   multiIf(any(bon.13) = 1, 'D', any(bon.13) = 2, 'C', any(bon.13) = 3, 'P', '') AS oper_type,
   if(ci_qty > 1, 0, any(ci.chequeitem_id)) AS chequeitem_id,
   if((uniqExact(ci.chequeitem_id) AS ci_qty) > 1, 0, any(ci.article_id)) AS article_id,
   any(arrayMap(x -> (x.1, x.2), ci.ea_ci)) AS ea_ci,
   any(ch.cheque_id) AS cheque_id,
   any(ch.shop_id) AS shop_id,
   any(bon.9) AS card_id,
   cityHash64(card_id, instance_id, get_salt(instance_id)) AS card_hash,
   0 AS is_order,
   anyIf(bon, bon.6 = 5).7 AS credit_bonus_id,
   toDateTime(any(bon.14), 'UTC') AS dt_start_date,
   toDateTime(any(bon.15), 'UTC') AS dt_finish_date,
   any(bon.17) AS remainder,
   now() AS dt_load,
   any(ch.summ) AS cheque_summ,
   any(ch.summdisc) AS cheque_summdiscounted,
   get_partner(0, instance_id, organization_id, 0) as tenant_id,
	if(ci_qty > 1, 0, any(ci.quantity)) AS ci_quantity,
    if(ci_qty > 1, 0, any(ci.summ)) AS ci_summ,
    if(ci_qty > 1, 0, any(ci.summdiscounted)) AS ci_summdiscounted
from stage.ci_temporary_new ci
semi left join stage.ci_temporary_ch_new ch on ch.cheque_id = ci.cheque_id and ch.instance_id = ci.instance_id
WHERE (arrayConcat(ci.bo_ci, ch.bo_ch) AS bo) > []
GROUP BY
    b_instance_hash,
    ci.instance_id
SETTINGS
max_memory_usage = '80G',
max_bytes_before_external_group_by = '7G',
max_bytes_before_external_sort = '7G',
optimize_on_insert = 0,
max_threads = 6,
max_insert_threads = 6,
join_algorithm = 'partial_merge' ,
enable_software_prefetch_in_aggregation =1,
allow_aggregate_partitions_independently =1,
force_aggregate_partitions_independently =1,
enable_writes_to_query_cache = false;



insert into dwh.bonus_slim_daily
(    b_instance_hash,instance_id,bonus_id,organization_id,source_table,is_delete,value,dt_created,parent_type_id,parent_id,rule_id,campaign_id,is_status,oper_type,chequeitem_id,article_id
    ,ea_ci,cheque_id,shop_id,card_id,card_hash,is_order,credit_bonus_id,dt_start_date,dt_finish_date,remainder,dt_load,cheque_summ,cheque_summdiscounted,tenant_id,ci_quantity,ci_summ,ci_summdiscounted)
WITH (dt >= (now() - toIntervalHour(6))) AND (dt <= now()) AS b_where
SELECT
    key_id AS b_instance_hash,
    (argMax((is_del, instance_id, source_table), last_version) AS bot1).2 AS instance_id_,
    (argMaxIf((chequeitem_id, cheque_id, card_id, bonus_id, bonus_wo_id, created_on, organization_id, value, parent_type_id, parent_id, rule_id, campaign_id, is_status, operation_type_id, is_pred_order, start_date, finish_date, remainder, time_load), last_version, is_del = 0) AS bot2).4 AS bonus_id_,
    bot2.7 AS organization_id_,
    toUInt8(bot1.3) AS source_table_,
    bot1.1 AS is_delete_,
    bot2.8 AS value_,
    toDateTime(bot2.6, 'UTC') AS dt_created,
    bot2.9 AS parent_type_id_,
    bot2.10 AS parent_id_,
    bot2.11 AS rule_id_,
    bot2.12 AS campaign_id_,
    bot2.13 AS is_status_,
    bot2.14 AS oper_type,
    bot2.1 AS chequeitem_id_,
    0 AS article_id,
    [] AS ea_ci,
    bot2.2 AS cheque_id_,
    0 AS shop_id,
    bot2.3 AS card_id_,
    cityHash64(card_id_, instance_id_, get_salt(instance_id_)) AS card_hash,
    (bot2.15) != 0 AS os_order,
    bot2.5 AS credit_bonus_id,
    toDateTime(bot2.16, 'UTC') AS dt_start_date,
    toDateTime(bot2.17, 'UTC') AS dt_finish_date,
    bot2.18 AS remainder_,
    bot2.19 AS dt_load,
    0 AS cheque_summ,
    0 AS cheque_summdiscounted,
    get_partner(0, instance_id_, organization_id_, 0) as tenant_id,
    0 AS ci_quantity,
    0 AS ci_summ,
    0 AS ci_summdiscounted
FROM stage.stage_bonus
WHERE key_id IN (
    SELECT key_id
    FROM stage.slim_tables_load_log
    WHERE b_where
)
GROUP BY key_id
HAVING (NOT ((bot2.1) OR (bot2.2))) AND (bot2.3) AND (bot2.4)
SETTINGS
max_memory_usage = '80G',
max_bytes_before_external_group_by = '7G',
max_bytes_before_external_sort = '7G',
optimize_on_insert = 0,
max_threads = 6,
max_insert_threads = 6,
join_algorithm = 'partial_merge' ,
enable_software_prefetch_in_aggregation =1,
allow_aggregate_partitions_independently =1,
force_aggregate_partitions_independently =1,
enable_writes_to_query_cache = false;


select formatReadableSize(total_bytes), * from system.tables
where table = 'set_bo';


select pb, pe, formatReadableQuantity(count())
from stage.bo_log
group by pb, pe with rollup
order by pb desc;

select
    formatReadableQuantity(count())
    , formatReadableQuantity(uniq(key_hash))
    , formatReadableQuantity(uniq((key_hash, attribute_hash)))
from stage.bo_log
limit 1;

optimize table stage.bo_log deduplicate by *, dt_load;

select formatReadableSize(result_bytes), *
from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and http_user_agent = 'curl/7.64.0'
    and hasAny(['stage.bo_keys', 'stage.bo_log', 'stage.set_bo','service.qwe', 'stage.bo'], tables)
--     and query like '%bo%'
order by event_time desc;

show processlist;

with toDateTime('2024-08-27 00:00:00') as pb, 2000000 as rows
-- select greatest(max(dt_load), toDateTime('2024-08-27 00:00:00')) as maxdt from (
    select dt_load from stage.bo_keys where dt_load > pb order by dt_load limit rows
--     );

select toStartOfHour(dt_load) as dt_load, count()
from stage.bo_keys
where toDate(dt_load) = today()
group by dt_load
order by dt_load;



select count()
from stage.bo
where key_hash in stage.set_bo;

select
    table
    , formatReadableSize(sum(bytes_on_disk)) as bytes
    , formatReadableQuantity(sum(rows)) as row
from system.parts
where database = 'stage'
    and table in ('bo_keys', 'bo_log')
    and active
group by table;

optimize table stage.bo_keys final deduplicate by key_hash,related_hash,attribute_hash,source_table,ym;
optimize table stage.bo_log final deduplicate by key_hash,attribute_hash, ym;

select count() from stage.bo_log;


with
    toDateTime('2024-08-26 16:17:46') as pb
    , 2000000 as rows
select greatest(max(dt_load), toDateTime('2024-08-03 23:34:47')) as maxdt
from
(
    select
        dt_load
    from stage.bo_keys
    where dt_load > pb
    order by dt_load
    limit rows
);



