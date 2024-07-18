insert into dwh.chequeitems_daily
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load between pb and pe as dt_where
    , rs as
    (
        select
            (argMaxIf(tuple(* except (key_hash, is_del, last_version), dt_load), last_version, not is_del) as tup).1 as instance_id
            , tup.2 as source_table
            , tup.3 as is_cheque
            , tup.4 as cheque_id
            , tup.5 as ch_number
            , tup.6 as dt
            , tup.7 as oper_type
            , tup.8 as card_id
            , tup.9 as shop_id
            , tup.10 as organization_id
            , tup.11 as pos_id
            , tup.12 as summ_ch
            , tup.13 as summdisc_ch
            , tup.14 as chequeitem_id
            , tup.15 as article_id
            , tup.16 as price
            , tup.17 as quantity
            , tup.18 as summdisc
            , tup.19 as summ
            , tup.20 as discount
            , tup.21 as position_number
            , tup.22 as paid_by_bonus
            , tup.23 as mcp
            , tup.24 as article_external_id
            , tup.25 as coupon_num
            , tup.26 as ea_key
            , tup.27 as ea_value
            , tup.28 as value
            , tup.29 as rule_id
            , tup.30 as campaign_id
            , tup.31 as process_dt
            , tup.32 as is_status
            , tup.33 as oper_type_b
            , tup.34 as parent_type
            , tup.35 as paymenttype_id
            , tup.36 as payment_value
            , tup.37 as dt_load_
            , argMax(is_del, last_version) as del
        , key_hash
        from service.ci
        where key_hash in
        (
            select arrayJoin([key_hash, related_hash])
            from service.ci_keys
            where key_hash in
            (
                select arrayJoin([key_hash, related_hash])
                from service.ci_keys
                where key_hash in
                (
                    select
                        arrayJoin([key_hash, related_hash])
                    from service.ci_keys
                    where key_hash in
                    (
                        select
                            arrayJoin([key_hash, related_hash])
                        from service.ci_keys
                        where key_hash in
                        (
                            select
                                arrayJoin([key_hash, related_hash])
                            from service.ci_keys
                            where dt_where
                            and (key_hash, attribute_hash) not in
                            (
                                select key_hash, attribute_hash
                                from service.ci_log
                                where key_hash in (select key_hash from service.ci_keys where dt_where)
                            )
                        )
                    )
                )
            )
        ) group by key_hash
        having instance_id and (source_table in (1, 2) or del = 0)
    )
select
    instance_id
    , ch.ch.2 as ch_number
    , ch.ch.3 as dt
    , toYYYYMM(toDateTime(dt)) as ym
    , ch.ch.4 as oper_type
    , ci.ci.3 as article_id
    , ci.ci.4 as price
    , ci.ci.5 as quantity
    , ci.ci.6 as summdisc
    , ci.ci.7 as summ
    , ci.ci.8 as discount
    , ch.cheque_id as cheque_id
    , ci.chequeitem_id as chequeitem_id
    , ch.ch.5 as card_id
    , cityHash64(card_id, instance_id, get_salt(instance_id)) as card_hash
    , ch.cou as coupon_num
    , ch.ea_ch as ea_ch
    , arrayMap(x ->(
            x.1
            , assumeNotNull(JSONExtract(x.2, 'Tuple(N Nullable(String))').1)
            , assumeNotNull(JSONExtract(x.2, 'Tuple(D Nullable(Float32))').1)
            , assumeNotNull(JSONExtract(x.2, 'Tuple(Q Nullable(Float32))').1)
            , x.2
        ) , ci.ea_ci) as ea_ci
    , arrayFilter
        (
            y -> y != 0
            , arrayMap
                (
                    x -> dictGet('dwh.d_rule_by_external_id', 'campaign_id', cityHash64(replaceAll(replaceAll(lower(x), 'disc', ''), 'di', ''), instance_id))
                    , arrayFlatten([ea_ci.1, ea_ch.1])
                )
        ) as campaign_id
    , ch.b_ch as bo_ch
    , ci.b_ci as bo_ci
    , ch.ch.6 as shop_id
    , ch.ch.7 as organization_id
    , ch.ch.8 as pos_id
    , ch.ch.9 as summ_ch
    , ch.ch.10 as summdisc_ch
    , greatest(ch.ch.1, ci.ci.2) as is_del
    , (dictGet('dwh.d_card', tuple('contact_id', 'card_type_id'), card_hash) as card_attrs).1 AS contact_id
    , pe as dt_load
    , toDate(dt_load) as d_load
    , get_partner(0, instance_id, organization_id, 0) as tenant_id
    , ci.ci.9 as position_number
    , ci.ci.10 as paid_by_bonus
    , ci.ci.11 as mcp
    , ch.pay as payment_type
    , card_attrs.2 as card_type_id
    , ci.ci.12 as article_external_id
from
(
    select
        chequeitem_id
        , instance_id
        , anyIf((cheque_id, del, article_id, price, quantity, summdisc, summ, discount, position_number, paid_by_bonus, mcp, article_external_id), source_table = 1) as ci
        , groupArrayIf((ea_key, ea_value), source_table = 31) as ea_ci
        , groupArrayIf((value, rule_id, campaign_id, process_dt, is_status, source_table = 51 ? 5 : 6/*, oper_type_b, parent_type*/), source_table in (51, 61)) as b_ci
        , max(dt_load_) as dt_load
    from rs
    where not is_cheque
    group by chequeitem_id, instance_id
    having has(groupArray(source_table), 1)
) as ci
semi left join
(
    select
        cheque_id
        , instance_id
        , anyIf((del, ch_number, dt, oper_type, card_id, shop_id, organization_id, pos_id, summ_ch, summdisc_ch), source_table = 2) as ch
        , groupArrayIf((ea_key, ea_value), source_table = 32) as ea_ch
        , groupUniqArrayIf((value, rule_id, campaign_id, process_dt, is_status, source_table = 52 ? 5 : 6/*, oper_type_b, parent_type*/), source_table in (52, 62)) as b_ch
        , groupArrayIf((paymenttype_id, payment_value), source_table = 4) as pay
        , groupArrayIf(coupon_num, source_table = 7) as cou
        , max(dt_load_) as dt_load
    from rs
    where is_cheque
    group by cheque_id, instance_id
    having has(groupArray(source_table), 2)
) as ch on ch.cheque_id = ci.ci.1 and ch.instance_id = ci.instance_id;
