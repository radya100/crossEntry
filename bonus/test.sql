select
    name, type
from system.columns
where database = 'dwh'
    and table = 'bonus_slim_daily'
    and default_kind not in ('MATERIALIZED', 'ALIAS')
order by position;

select *
from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and query like '%bonus_slim_daily%';


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


-- qwe


select source_table
from stage.bo_keys
group by source_table;

select formatReadableSize(total_bytes), * from system.tables
where table = 'set_bo';



1    , instance_id
2    , source_table
3    , is_header
31    , dt_load
-->> bonus
4    , bonus_id
5    , credit_bonus_id
6    , organization_id
7    , value Int64
8    , dt_created
9   , parent_type_id
10   , parent_id
11   , rule_id
12    , campaign_id
13    , is_status
14    , oper_type
15    , dt_start_date
16    , dt_finish_date
17    , is_order
18    , remainder
-->> CI
19    , chequeitem_id
20    , article_id
21    , ci_quantity
22    , ci_summ
23    , ci_summdiscounted
-->> EA_CI
24    , ea_key
25    , ea_value
-->> CH
26    , cheque_id
27    , shop_id
28    , card_id
29    , cheque_summ
30    , cheque_summdiscounted

;select *
from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type not in ('QueryStart', 'QueryFinish')
order by event_time desc;

with rs as(    select        (argMaxIf(tuple(* except (key_hash, is_del, last_version), dt_load), last_version, not is_del) as tup).1 as instance_id        , tup.2 as source_table        , tup        , argMax(is_del, last_version) as del    from stage.bo    where key_hash in stage.set_bo    group by key_hash    having instance_id and (source_table in (5, 6) or del = 0))select    cityHash64() as b_instance_hash    , s1.instance_id as instance_id    , s1.tup.4 as bonus_id    , s1.tup.6 as organization_id    , s1.tup.2 as source_table    , s1.del as is_delete    , s1.tup.7 as value    , s1.tup.8 as dt_created    , s1.tup.9 as parent_type_id    , s1.tup.10 as parent_id    , s1.tup.11 as rule_id    , cityHash64(rule_id, instance_id) as rule_instance_hash    , s1.tup.12 as campaign_id    , cityHash64(campaign_id, instance_id) as campaign_instance_hash    , s1.tup.13 as is_status    , s1.tup.14 as oper_type    , s3.chequeitem_id as chequeitem_id    , s3.tup_ci.1 as article_id    , cityHash64(article_id, instance_id, 4) as article_instance_hash    , arrayMap(x -> (x.1, x.2), s4.tup_ea) as ea_ci    , s2.cheque_id as cheque_id    , s2.tup_ch.1 as shop_id    , cityHash64(shop_id, instance_id) as shop_instance_hash    , get_partner(0, instance_id, organization_id, 0) as tenant_id    , s2.tup_ch.2 as card_id    , cityHash64(card_id, instance_id, get_salt(instance_id)) as card_hash    , cityHash64(card_id, instance_id) as card_instance_hash    , s1.tup.17 as is_order    , s1.tup.5 as credit_bonus_id    , s1.tup.15 as dt_start_date    , s1.tup.16 as dt_finish_date    , s1.tup.18 as remainder    , greatest(s1.tup.31, s2.tup_ch.5, s3.tup_ci.5, toDateTime(arrayMax(x -> toUInt64(x), s4.tup_ea.3))) as dt_load    , s2.tup_ch.3 as cheque_summ    , s2.tup_ch.4 as cheque_summdiscounted    , s3.tup_ci.2 as ci_quantity    , s3.tup_ci.3 as ci_summ    , s3.tup_ci.4 as ci_summdiscountedfrom(    select        tup        , tup.26 as cheque_id        , tup.19 as chequeitem_id        , instance_id        , del    from rs    where source_table in (5, 6)) as s1any left join(    select        tup.26 as cheque_id        , (tup.27, tup.28, tup.29, tup.30, tup.31) as tup_ch        , instance_id    from rs    where source_table = 2) as s2 on s2.cheque_id = s1.cheque_id and s2.instance_id = s1.instance_idany left join(    select        tup.19 as chequeitem_id        , instance_id        , (tup.20, tup.21 , tup.22, tup.23, tup.31) as tup_ci    from rs    where source_table = 1) as s3 on s3.chequeitem_id = s1.chequeitem_id and  s3.instance_id = s1.instance_idany left join(    select        tup.19 as chequeitem_id        , instance_id        , groupArray((tup.24, tup.25, tup.31)) as tup_ea    from rs    where source_table = 3    group by chequeitem_id, instance_id) as s4 on s4.chequeitem_id = s1.chequeitem_id and  s4.instance_id = s1.instance_idformat TSVWithNames

;create table service.qwe engine = Log() as select * from dwh.bonus_slim_retro limit 0;
select * from service.qwe;
truncate table service.qwe;

select *
from system.query_log
where event_date = today()
    and user = 'airflow_user'
--     and type <> 'QueryStart'
    and query like '%qwe%'
order by event_time desc