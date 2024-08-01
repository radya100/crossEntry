with rs as
(
    select
        (argMaxIf(tuple(* except (key_hash, is_del, last_version), dt_load), last_version, not is_del) as tup).1 as instance_id
        , tup.2 as source_table
        , tup
        , argMax(is_del, last_version) as del
    from stage.bo
    where key_hash in stage.set_bo
    group by key_hash
    having instance_id and (source_table in (5, 6) or del = 0)
)
select
    cityHash64() as b_instance_hash
    , s1.instance_id as instance_id
    , s1.tup.4 as bonus_id
    , s1.tup.6 as organization_id
    , s1.tup.2 as source_table
    , s1.del as is_delete
    , s1.tup.7 as value
    , s1.tup.8 as dt_created
    , s1.tup.9 as parent_type_id
    , s1.tup.10 as parent_id
    , s1.tup.11 as rule_id
    , cityHash64(rule_id, instance_id) as rule_instance_hash
    , s1.tup.12 as campaign_id
    , cityHash64(campaign_id, instance_id) as campaign_instance_hash
    , s1.tup.13 as is_status
    , s1.tup.14 as oper_type
    , s3.chequeitem_id as chequeitem_id
    , s3.tup_ci.1 as article_id
    , cityHash64(article_id, instance_id, 4) as article_instance_hash
    , arrayMap(x -> (x.1, x.2), s4.tup_ea) as ea_ci
    , s2.cheque_id as cheque_id
    , s2.tup_ch.1 as shop_id
    , cityHash64(shop_id, instance_id) as shop_instance_hash
    , get_partner(0, instance_id, organization_id, 0) as tenant_id
    , s2.tup_ch.2 as card_id
    , cityHash64(card_id, instance_id, get_salt(instance_id)) as card_hash
    , cityHash64(card_id, instance_id) as card_instance_hash
    , s1.tup.17 as is_order
    , s1.tup.5 as credit_bonus_id
    , s1.tup.15 as dt_start_date
    , s1.tup.16 as dt_finish_date
    , s1.tup.18 as remainder
    , greatest(s1.tup.31, s2.tup_ch.5, s3.tup_ci.5, toDateTime(arrayMax(x -> toUInt64(x), s4.tup_ea.3))) as dt_load
    , s2.tup_ch.3 as cheque_summ
    , s2.tup_ch.4 as cheque_summdiscounted
    , s3.tup_ci.2 as ci_quantity
    , s3.tup_ci.3 as ci_summ
    , s3.tup_ci.4 as ci_summdiscounted
from
(
    select
        tup
        , tup.26 as cheque_id
        , tup.19 as chequeitem_id
        , instance_id
        , del
    from rs
    where source_table in (5, 6)
) as s1
any left join
(
    select
        tup.26 as cheque_id
        , (tup.27, tup.28, tup.29, tup.30, tup.31) as tup_ch
        , instance_id
    from rs
    where source_table = 2
) as s2 on s2.cheque_id = s1.cheque_id and s2.instance_id = s1.instance_id
any left join
(
    select
        tup.19 as chequeitem_id
        , instance_id
        , (tup.20, tup.21 , tup.22, tup.23, tup.31) as tup_ci
    from rs
    where source_table = 1
) as s3 on s3.chequeitem_id = s1.chequeitem_id and  s3.instance_id = s1.instance_id
any left join
(
    select
        tup.19 as chequeitem_id
        , instance_id
        , groupArray((tup.24, tup.25, tup.31)) as tup_ea
    from rs
    where source_table = 3
    group by chequeitem_id, instance_id
) as s4 on s4.chequeitem_id = s1.chequeitem_id and  s4.instance_id = s1.instance_id
format TSVWithNames