create table stage.chi on cluster basic
(
    key_hash UInt128 codec(DoubleDelta, ZSTD(3))
    , instance_id UInt16
    , source_table UInt8
    , is_cheque UInt8
    , is_del	UInt8
    , last_version Int64 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))
--cheque
    , cheque_id	Int64 codec(DoubleDelta, ZSTD(3))
    , ch_number String codec(DoubleDelta, ZSTD(3))
    , dt Int64 codec(DoubleDelta, ZSTD(3))
    , oper_type	Int32 codec(DoubleDelta, ZSTD(3))
    , card_id	Int32 codec(DoubleDelta, ZSTD(3))
    , shop_id	Int32 codec(DoubleDelta, ZSTD(3))
    , organization_id Int32 codec(DoubleDelta, ZSTD(3))
    , pos_id	Int32 codec(DoubleDelta, ZSTD(3))
    , summ_ch	Int64 codec(ZSTD(3))
    , summdisc_ch	Int64 codec(ZSTD(3))
--item
    , chequeitem_id	Int64 codec(DoubleDelta, ZSTD(3))
    , article_id Int32 codec(DoubleDelta, ZSTD(3))
    , price	Int64 codec(ZSTD(3))
    , quantity	Int64 codec(ZSTD(3))
    , summdisc	Int64 codec(ZSTD(3))
    , summ	Int64 codec(ZSTD(3))
    , discount	Int64 codec(ZSTD(3))
    , position_number	String codec(DoubleDelta, ZSTD(3))
    , paid_by_bonus	Int64 codec(DoubleDelta, ZSTD(3))
    , mcp Int64 codec(DoubleDelta, ZSTD(3))
    , article_external_id String codec(DoubleDelta, ZSTD(3))
--coupon
    , coupon_num String codec(DoubleDelta, ZSTD(3))
--ea_all
    , ea_key String codec(ZSTD(3))
    , ea_value String codec(ZSTD(3))
--bonus
    , value Int64 codec(ZSTD(3))
    , rule_id Int32 codec(DoubleDelta, ZSTD(3))
    , campaign_id Int32 codec(DoubleDelta, ZSTD(3))
    , process_dt Int64 codec(DoubleDelta, ZSTD(3))
    , is_status UInt8 codec(DoubleDelta, ZSTD(3))

    , oper_type_b String codec(DoubleDelta, ZSTD(3))
    , parent_type Int32 codec(DoubleDelta, ZSTD(3))
--payment
    , paymenttype_id Int32 codec(DoubleDelta, ZSTD(3))
    , payment_value Int64 codec(ZSTD(3))
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_chi', '{replica}', last_version)
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;


create table stage.chi_keys on cluster basic
(
    key_hash UInt128 codec(DoubleDelta, ZSTD(3))
    , related_hash UInt128 codec(DoubleDelta, ZSTD(3))
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_chi_keys', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

create table stage.chi_log on cluster basic
(
    key_hash UInt128 codec(DoubleDelta, ZSTD(3))
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime
    , pe DateTime
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_chi_log', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

--=====>CH
drop table null.mv_to_stage_chi_from_cheque on cluster basic;
create materialized view null.mv_to_stage_chi_from_cheque on cluster basic to stage.chi as
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
select
    init_key + assumeNotNull(cheque_Id) as key_hash
    , instance_id
    , source_table
    , 1 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version
--cheque
    , assumeNotNull(cheque_Id) as cheque_id
    , assumeNotNull(number) as ch_number
    , assumeNotNull(datetime) as dt
    , assumeNotNull(operationtype_id) as oper_type
    , assumeNotNull(card_id) as card_id
    , assumeNotNull(orgunit_id) as shop_id
    , assumeNotNull(organization_id) as organization_id
    , assumeNotNull(pos_id) as pos_id
    , assumeNotNull(summ) as summ_ch
    , assumeNotNull(summdiscounted) as summdisc_ch
from null.loyalty__null__loyalty__cheque_cur;

drop table null.mv_to_stage_chi_keys_from_cheque on cluster basic;
create materialized view null.mv_to_stage_chi_keys_from_cheque on cluster basic to stage.chi_keys as
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
select
    init_key + assumeNotNull(cheque_Id) as key_hash
    , init_key + assumeNotNull(cheque_Id) as related_hash
    , cityHash64
        (
            assumeNotNull(cheque_Id)
            , assumeNotNull(number)
            , assumeNotNull(datetime)
            , assumeNotNull(operationtype_id)
            , assumeNotNull(card_id)
            , assumeNotNull(orgunit_id)
            , assumeNotNull(organization_id)
            , assumeNotNull(pos_id)
            , assumeNotNull(summ)
            , assumeNotNull(summdiscounted)
        ) as attribute_hash
    , source_table
from null.loyalty__null__loyalty__cheque_cur;

--=====CI
drop table null.mv_to_stage_chi_from_chequeitem on cluster basic;
create materialized view null.mv_to_stage_chi_from_chequeitem on cluster basic to stage.chi as
with 1 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
select
    init_key + assumeNotNull(chequeitem_Id) as key_hash
    , instance_id
    , source_table
    , 0 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(chequeitem_Id) as chequeitem_id
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(article_id) as article_id
    , assumeNotNull(price) as price
    , assumeNotNull(quantity) as quantity
    , assumeNotNull(summdiscounted) as summdisc
    , assumeNotNull(summ) as summ
    , assumeNotNull(discount) as discount
    , assumeNotNull(number) as position_number
    , assumeNotNull(paid_by_bonus) as paid_by_bonus
    , assumeNotNull(mcp) as mcp
    , assumeNotNull(articlenumber) as article_external_id
from null.loyalty__null__loyalty__chequeitem_cur;

drop table null.mv_to_stage_chi_keys_from_chequeitem on cluster basic;
create materialized view null.mv_to_stage_chi_keys_from_chequeitem on cluster basic to stage.chi_keys as
with 1 as source_table
    , 2 as related_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * related_table)
        + toUInt128(9223372036854775808) as init_related_key
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(h1, h1)]
                : [((init_key + assumeNotNull(chequeitem_Id) as h1), (init_related_key + assumeNotNull(cheque_id) as h2)), (h2, h1)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
        (
            assumeNotNull(chequeitem_Id)
            , assumeNotNull(article_id)
            , assumeNotNull(price)
            , assumeNotNull(quantity)
            , assumeNotNull(summdiscounted)
            , assumeNotNull(summ)
            , assumeNotNull(discount)
            , assumeNotNull(number)
            , assumeNotNull(paid_by_bonus)
            , assumeNotNull(mcp)
            , assumeNotNull(articlenumber)
        ) as attribute_hash
    , source_table
from null.loyalty__null__loyalty__chequeitem_cur;

--====> EA
drop table null.mv_to_stage_chi_from_ea on cluster basic;
create materialized view null.mv_to_stage_chi_from_ea on cluster basic to stage.chi as
with 31 as source_table_ci
    , 32 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(extended_attributes_id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(extended_attributes_id) as init_key_ch
select
    tup.1 as key_hash
    , instance_id
    , tup.2 as source_table
    , tup.3 as is_cheque
    , last_version
    , (arrayJoin(
        multiIf(
            sys_change_operation = 'D' , [(init_key_ci, source_table_ci, 0, 1), (init_key_ch, source_table_ch, 1, 1)]
            , assumeNotNull(cheque_id) = 0, [(init_key_ci, source_table_ci, 0, 0)]
            , [(init_key_ch, source_table_ch, 1, 0)]
        )
    ) as tup).4 as is_del
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(chequeitem_id) as chequeitem_id
    , assumeNotNull(key) as ea_key
    , assumeNotNull(value) as ea_value
from null.loyalty__null__loyalty__extended_attributes_cur;

drop table null.mv_to_service_chi_keys_from_ea on cluster basic;
create materialized view null.mv_to_service_chi_keys_from_ea on cluster basic to stage.chi_keys as
with 31 as source_table_ea_ci
    , 32 as source_table_ea_ch
    , 1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ea_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(extended_attributes_id) as init_key_ea_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ea_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(extended_attributes_id) as init_key_ea_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeitem_id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_id) as init_key_ch
select
    (arrayJoin(
        multiIf(
            sys_change_operation = 'D' , [(init_key_ea_ci, init_key_ea_ci, source_table_ea_ci), (init_key_ea_ch, init_key_ea_ch, source_table_ea_ch)]
            , assumeNotNull(cheque_id) = 0, [(init_key_ea_ci, init_key_ci, source_table_ea_ci), (init_key_ci, init_key_ea_ci, source_table_ea_ci)]
            , [(init_key_ea_ch, init_key_ch, source_table_ea_ch), (init_key_ch, init_key_ea_ch, source_table_ea_ch)]
        )
    ) as tup).1 as key_hash
    , tup.2 as related_hash
    , cityHash64(
        assumeNotNull(key)
        , assumeNotNull(value)
    ) as attribute_hash
    , tup.3 as source_table
from null.loyalty__null__loyalty__extended_attributes_cur;

--====> BO
drop table null.mv_to_stage_chi_from_bo on cluster basic;
create materialized view null.mv_to_stage_chi_from_bo on cluster basic to stage.chi as
with 61 as source_table_ci
    , 62 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_id) as init_key_ch
select
    tup.1 as key_hash
    , instance_id
    , tup.2 as source_table
    , tup.3 as is_cheque
    , (arrayJoin(
        multiIf(
            sys_change_operation = 'D' , [(init_key_ci, source_table_ci, 0, 1), (init_key_ch, source_table_ch, 1, 1)]
            , assumeNotNull(cheque_item_id) = 0, [(init_key_ch, source_table_ch, 1, 0)]
            , [(init_key_ci, source_table_ci, 0, 0)]
        )
    ) as tup).4 as is_del
    , last_version
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(cheque_item_id) as chequeitem_id
    , assumeNotNull(value) as value
    , assumeNotNull(rule_id) as rule_id
    , assumeNotNull(campaign_id) as campaign_id
    , assumeNotNull(processed_date) as process_dt
    , assumeNotNull(isStatus) as is_status
    , assumeNotNull(operation_type_id) as oper_type_b
    , assumeNotNull(parent_type_id) as parent_type
from null.loyalty__null__loyalty__bonus_cur
where (sys_change_operation = 'D' or cheque_id is not null);

drop table null.mv_to_stage_chi_keys_from_bo on cluster basic;
create materialized view null.mv_to_stage_chi_keys_from_bo on cluster basic to stage.chi_keys as
with 61 as source_table_bo_ci
    , 62 as source_table_bo_ch
    , 1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_bo_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_id) as init_key_bo_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_bo_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_id) as init_key_bo_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_item_id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_id) as init_key_ch
select
    (arrayJoin(
        multiIf(
            sys_change_operation = 'D' , [(init_key_bo_ci, init_key_bo_ci, source_table_bo_ci), (init_key_bo_ch, init_key_bo_ch, source_table_bo_ch)]
            , assumeNotNull(cheque_item_id) = 0, [(init_key_bo_ch, init_key_ch, source_table_bo_ch), (init_key_ch, init_key_bo_ch, source_table_bo_ch)]
            , [(init_key_bo_ci, init_key_ci, source_table_bo_ci), (init_key_ci, init_key_bo_ci, source_table_bo_ci)]
        )
    ) as tup).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
    (
        assumeNotNull(value) as value
        , assumeNotNull(rule_id) as rule_id
        , assumeNotNull(campaign_id) as campaign_id
        , assumeNotNull(processed_date) as process_dt
        , assumeNotNull(isStatus) as is_status
        , assumeNotNull(operation_type_id) as oper_type_b
        , assumeNotNull(parent_type_id) as parent_type
    ) as attribute_hash
    , tup.3 as source_table
from null.loyalty__null__loyalty__bonus_cur
where (sys_change_operation = 'D' or cheque_id is not null);

--====> BW
drop table if exists null.mv_to_stage_chi_from_bw on cluster basic;
create materialized view null.mv_to_stage_chi_from_bw on cluster basic to stage.chi as
with 51 as source_table_ci
    , 52 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_wo_id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_wo_id) as init_key_ch
select
    tup.1 as key_hash
    , instance_id
    , tup.2 as source_table
    , tup.3 as is_cheque
    , (arrayJoin(
        multiIf(
            sys_change_operation = 'D' , [(init_key_ci, source_table_ci, 0, 1), (init_key_ch, source_table_ch, 1, 1)]
            , assumeNotNull(cheque_item_id) = 0, [(init_key_ch, source_table_ch, 1, 0)]
            , [(init_key_ci, source_table_ci, 0, 0)]

        )
    ) as tup).4 as is_del
    , last_version
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(cheque_item_id) as chequeitem_id
    , assumeNotNull(value) as value
    , assumeNotNull(rule_id) as rule_id
    , assumeNotNull(campaign_id) as campaign_id
    , assumeNotNull(processed_date) as process_dt
    , assumeNotNull(isStatus) as is_status
    , assumeNotNull(operation_type_id) as oper_type_b
    , assumeNotNull(parent_type_id) as parent_type
from null.loyalty__null__loyalty__bonus_wo_cur
where (sys_change_operation = 'D' or cheque_id is not null);

drop table null.mv_to_stage_chi_keys_from_bw on cluster basic;
create materialized view null.mv_to_stage_chi_keys_from_bw on cluster basic to stage.chi_keys as
with 51 as source_table_bo_ci
    , 52 as source_table_bo_ch
    , 1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_bo_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_wo_id) as init_key_bo_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_bo_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_wo_id) as init_key_bo_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_item_id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_id) as init_key_ch
select
    (arrayJoin(
        multiIf(
            sys_change_operation = 'D' , [(init_key_bo_ci, init_key_bo_ci, source_table_bo_ci), (init_key_bo_ch, init_key_bo_ch, source_table_bo_ch)]
            , assumeNotNull(cheque_item_id) = 0, [(init_key_bo_ch, init_key_ch, source_table_bo_ch), (init_key_ch, init_key_bo_ch, source_table_bo_ch)]
            , [(init_key_bo_ci, init_key_ci, source_table_bo_ci), (init_key_ci, init_key_bo_ci, source_table_bo_ci)]

        )
    ) as tup).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
    (
        assumeNotNull(value) as value
        , assumeNotNull(rule_id) as rule_id
        , assumeNotNull(campaign_id) as campaign_id
        , assumeNotNull(processed_date) as process_dt
        , assumeNotNull(isStatus) as is_status
        , assumeNotNull(operation_type_id) as oper_type_b
        , assumeNotNull(parent_type_id) as parent_type
    ) as attribute_hash
    , tup.3 as source_table
from null.loyalty__null__loyalty__bonus_wo_cur
where (sys_change_operation = 'D' or cheque_id is not null);

--====> PAYMENT
drop table null.mv_to_stage_chi_from_payment on cluster basic;
create materialized view null.mv_to_stage_chi_from_payment on cluster basic to stage.chi as
with 4 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
select
    init_key + assumeNotNull(payment_Id) as key_hash
    , instance_id
    , source_table
    , 1 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(paymenttype_id) as paymenttype_id
    , assumeNotNull(value) as payment_value
    , assumeNotNull(cheque_id) as cheque_id
from null.loyalty__null__loyalty__payment_cur;

drop table null.mv_to_stage_chi_keys_from_payment on cluster basic;
create materialized view null.mv_to_stage_chi_keys_from_payment on cluster basic to stage.chi_keys as
with 4 as source_table
    , 2 as related_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * related_table)
        + toUInt128(9223372036854775808) as init_related_key
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(h1, h1)]
                : [((init_key + assumeNotNull(payment_Id) as h1), (init_related_key + assumeNotNull(cheque_id) as h2)), (h2, h1)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
        (
            assumeNotNull(paymenttype_id) as paymenttype_id
            , assumeNotNull(value) as payment_value
        ) as attribute_hash
    , source_table
from null.loyalty__null__loyalty__payment_cur;

--====> COUPON
drop table if exists null.mv_to_stage_chi_from_coupon on cluster basic;
create materialized view null.mv_to_stage_chi_from_coupon on cluster basic to stage.chi as
with 7 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
select
    init_key + assumeNotNull(coupon_id) as key_hash
    , instance_id
    , source_table
    , 1 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(number) as coupon_num
    , assumeNotNull(cheque_id) as cheque_id
from null.loyalty__null__loyalty__coupon_cur
where (is_del or assumeNotNull(cheque_id) <> 0);

drop table null.mv_to_stage_chi_keys_from_coupon on cluster basic;
create materialized view null.mv_to_stage_chi_keys_from_coupon on cluster basic to stage.chi_keys as
with 7 as source_table
    , 2 as related_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808) as init_key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * related_table)
        + toUInt128(9223372036854775808) as init_related_key
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(h1, h1)]
                : [((init_key + assumeNotNull(coupon_id) as h1), (init_related_key + assumeNotNull(cheque_id) as h2)), (h2, h1)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
        (
            assumeNotNull(number)
        ) as attribute_hash
    , source_table
from null.loyalty__null__loyalty__coupon_cur
where (sys_change_operation = 'D' or assumeNotNull(cheque_id) <> 0);