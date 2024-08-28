select
    name, type
from system.columns
where database = 'stage'
    and table = 'bo'
    and default_kind not in ('MATERIALIZED', 'ALIAS')
    and name not in ('key_hash', 'is_del', 'last_version')
order by position;
--добавил коммент

show create dwh.bonus_slim_daily;

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

select * from system.tables where create_table_query like '%stage_bonus%';
show create null.mv_to_stage_bonus_from_;


select formatReadableSize(total_bytes), * from system.tables
where table = 'set_bo';


select pb, pe, formatReadableQuantity(count())
from stage.bo_log
group by pb, pe
order by pb desc;

select
    formatReadableQuantity(count())
    , formatReadableQuantity(uniq(key_hash))
    , formatReadableQuantity(uniq((key_hash, attribute_hash)))
from stage.bo_log
limit 1;

select formatReadableSize(result_bytes), *
from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and http_user_agent = 'curl/7.64.0'
    and hasAny(['stage.bo_keys', 'stage.bo_log', 'stage.set_bo','service.qwe', 'stage.bo', 'stage.bo_values'], tables)
order by event_time_microseconds desc;

show processlist;

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

select * from service.qwe where toDate(dt_load) = today()
    and shop_id = 0
limit 100;

show processlist ;

[['CouponName', 'nMg43CvdKL'], ['CouponName', 'Hn65VgRt5c'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'br45DcRw3k'], ['Coupon', 'Yes'], ['CouponName', 'Hn65VgRt5c'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BgvtYh5Nh'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', 'HB245129544'], ['Coupon', 'Yes'], ['CouponName', 'kMn45VhcEQ2'], ['CouponName', 'k8opepOA'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'MkGbh54Fvg'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'F6Wn1ieI'], ['CouponName', 'nK56Vb55Fb'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'VgrThnt654'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'tnh57BYf65'], ['Coupon', 'Yes'], ['CouponName', 'kMn45VhcEQ2'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'GRQnOYa7'], ['CouponName', 'b0zlK0gp'], ['CouponName', 'JwwT74Ad'], ['CouponName', 'jVn6lhMF'], ['Coupon', 'Yes'], ['CouponName', 'VgrThnt654'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'br45DcRw3k'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'o66Qnp38'], ['CouponName', '0Jco2BgA'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'mKh56Gvbrt'], ['CouponName', 'iPW8UydY'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'iPW8UydY'], ['CouponName', 'br45DcRw3k'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'gRt55Vbfc4'], ['Coupon', 'Yes'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'MkGtr54VvtrW'], ['Coupon', 'Yes'], ['CouponName', 'O2yIL56w'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', '3Gt4BgvHht'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'hNt65VbfcV'], ['CouponName', 'iPW8UydY'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', '3BnGtVb56B'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'uah7CGge'], ['Coupon', 'Yes'], ['CouponName', 'MHgeeuw2'], ['CouponName', 'MHgeeuw2'], ['CouponName', 'k8opepOA'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['CouponName', 'GuFNGLfQ2'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BnGtVb56B'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['CouponName', 'A3Wq0GO1'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BnGtVb56B'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'F6Wn1ieI'], ['CouponName', 'O2yIL56w'], ['CouponName', 'vc9dzAh9'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'uah7CGge'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'VTBDM'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'Promocode1708'], ['CouponName', 'VTBDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'O2yIL56w'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'F6Wn1ieI'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['CouponName', 'F6Wn1ieI'], ['CouponName', 'iPW8UydY'], ['CouponName', '2WdGt45Vbf'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'kMn45VhcEQ2'], ['CouponName', 'f5sRdOje'], ['CouponName', 'Promocode1708'], ['Coupon', 'Yes'], ['CouponName', 'b0zlK0gp'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', 'gLB8UUay'], ['CouponName', 'GuFNGLfQ2'], ['CouponName', '4GbhTr54BGf'], ['CouponName', 'pAyoCgW8'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'pAyoCgW8'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'F6Wn1ieI'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'A3Wq0GO1'], ['CouponName', 'VTBDM'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['CouponName', '6K02fR5tT'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', 'br45DcRw3k'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'd4GbTr54VvN'], ['Coupon', 'Yes'], ['CouponName', 'O2yIL56w'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BnGtVb56B'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'b0zlK0gp'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'N34DcvsGt5N'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', '3FvgTr554BH'], ['CouponName', 'DYNAMO1923'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'vc9dzAh9'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'gLB8UUay'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['CouponName', 'df35Vg76nnJ'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['CouponName', '1hU2BSd6d'], ['CouponName', 'kHj65Bh6S'], ['CouponName', 'VgrThnt654'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'gLB8UUay'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'mHn54FgTrV'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['CouponName', 'ChanganDM'], ['CouponName', 'GRQnOYa7'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'MHgeeuw2'], ['Coupon', 'Yes'], ['CouponName', 'HB249255645'], ['CouponName', 'MHgeeuw2'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'GRQnOYa7'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', 'F6Wn1ieI'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '2WdGt45Vbf'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'kB0ds02G'], ['Coupon', 'Yes'], ['CouponName', '2WdGt45Vbf'], ['CouponName', 'z4wA6fcU'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'pAyoCgW8'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'A3Wq0GO1'], ['CouponName', 'VTBDM'], ['CouponName', '8UMZ5sSi'], ['CouponName', 'VTBDM'], ['CouponName', 'b0zlK0gp'], ['CouponName', '8NK2g14O'], ['CouponName', 'DYNAMO1923'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'k8opepOA'], ['CouponName', 'mk35fhbr5g'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'kB0ds02G'], ['Coupon', 'Yes'], ['CouponName', 'MkGbh54Fvg'], ['CouponName', 'dHgt66Bhtv'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', '2WdGt45Vbf'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'pAyoCgW8'], ['CouponName', 'MkGbh54Fvg'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BgvtYh5Nh'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BnGtVb56B'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'df35Vg76nnJ'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'bh4eSuwx7'], ['CouponName', 'O2yIL56w'], ['CouponName', 'F6Wn1ieI'], ['Coupon', 'Yes'], ['CouponName', 'Nm75Bg3DcW'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'DixyDM'], ['Coupon', 'Yes'], ['CouponName', 'br45DcRw3k'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '4mGYT35g'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', 'o66Qnp38'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', '2WdGt45Vbf'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', '8UMZ5sSi'], ['CouponName', 'O2yIL56w'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'vrB8gRa9'], ['CouponName', 'nK56Vb55Fb'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'mk35fhbr5g'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['CouponName', 'GRQnOYa7'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'TJX1Jnaw'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'bh4eSuwx7'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'gLB8UUay'], ['CouponName', 'pAyoCgW8'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['Coupon', 'Yes'], ['CouponName', 'VTBDM'], ['CouponName', 'br45DcRw3k'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['CouponName', 'GuFNGLfQ2'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'mFg54gVhw2'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '2WdGt45Vbf'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['CouponName', 'iPW8UydY'], ['CouponName', 'Y0NTdqSjW'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'kHj65Bh6S'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'kMn45VhcEQ2'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'pAyoCgW8'], ['CouponName', '3BgvtYh5Nh'], ['CouponName', 'N34DcvsGt5N'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['CouponName', 'N34DcvsGt5N'], ['CouponName', 'iPW8UydY'], ['CouponName', 'JwwT74Ad'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'nK56Vb55Fb'], ['CouponName', 'VgrThnt654'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'Ng54FvGr5h'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '2WdGt45Vbf'], ['CouponName', 'iPW8UydY'], ['CouponName', 'O2yIL56w'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', '8NK2g14O'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['CouponName', 'HB242823259'], ['CouponName', 'HB244351325'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['CouponName', 'vrB8gRa9'], ['CouponName', 'nMg43CvdKL'], ['Coupon', 'Yes'], ['CouponName', 'BESTBENEFITSDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BgvtYh5Nh'], ['CouponName', 'mD35V6bNk'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'kMn45VhcEQ2'], ['CouponName', 'O4WLvGBXK'], ['CouponName', 'vQRA1S7K'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '3BgvTt67Btv'], ['Coupon', 'Yes'], ['CouponName', 'O2yIL56w'], ['CouponName', 'ChanganDM'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'A3Wq0GO1'], ['CouponName', '2WdGt45Vbf'], ['Coupon', 'Yes'], ['CouponName', 'iPW8UydY'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['Coupon', 'Yes'], ['CouponName', 'gRt55Vbfc4'], ['CouponName', 'N34DcvsGt5N'], ['Coupon', 'Yes'], ['CouponName', 'v3c3VKhf'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'nMg43CvdKL'], ['CouponName', '8UMZ5sSi'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'F6Wn1ieI'], ['Coupon', 'Yes'], ['CouponName', '8UMZ5sSi'], ['CouponName', '2ZNuiLxN'], ['CouponName', '0Jco2BgA'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'Hn65VgRt5c'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'DYNAMO1923'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['Coupon', 'Yes'], ['CouponName', 'F6Wn1ieI'], ['CouponName', 'F6Wn1ieI']]




