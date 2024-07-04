create table service.ci on cluster basic
(
    key_hash UInt128 -- key, instance, source
    , instance_id UInt16
    , source_table UInt8 -- (b = 6, bw = 5)
    , is_cheque UInt8
    , is_del	UInt8
    , last_version Int64
    , dt_load DateTime materialized now()
--cheque
    , cheque_id	Int64
    , ch_number String
    , dt Int64
    , oper_type	Int32
    , card_id	Int32
    , shop_id	Int32
    , organization_id	Int32
    , pos_id	Int32
    , summ_ch	Int64
    , summdisc_ch	Int64
--item
    , chequeitem_id	Int64
    , article_id Int32
    , price	Int64
    , quantity	Int64
    , summdisc	Int64
    , summ	Int64
    , discount	Int64
    , position_number	String
    , paid_by_bonus	Int64
    , mcp Int64
    , article_external_id String
--coupon
    , coupon_num String
--ea_all
    , ea_key String
    , ea_value String
--bonus
    , value Int64
    , rule_id Int32
    , campaign_id Int32
    , process_dt Int64
    , is_status UInt8

    , oper_type_b String
    , parent_type Int32
--payment
    , paymenttype_id Int32
    , payment_value Int64
) engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/ci', '{replica}', last_version) -- убери replacing !
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;

create table service.ci_keys on cluster basic
(
    key_hash UInt128
    , related_hash UInt128
    , attribute_hash UInt64
    , source_table UInt8
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/ci_keys', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

create table service.ci_log on cluster basic
(
    key_hash UInt128
    , attribute_hash UInt64
    , pb DateTime
    , pe DateTime
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/ci_log_1', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

--=====>CH
drop table null.mv_to_service_ci_from_cheque on cluster basic;
create materialized view null.mv_to_service_ci_from_cheque on cluster basic to service.ci as
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

drop table null.mv_to_service_ci_keys_from_cheque on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_cheque on cluster basic to service.ci_keys as
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
drop table null.mv_to_service_ci_from_chequeitem on cluster basic;
create materialized view null.mv_to_service_ci_from_chequeitem on cluster basic to service.ci as
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

drop table null.mv_to_service_ci_keys_from_chequeitem on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_chequeitem on cluster basic to service.ci_keys as
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
drop table null.mv_to_service_ci_from_ea on cluster basic;
create materialized view null.mv_to_service_ci_from_ea on cluster basic to service.ci as
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
-- from (select * from stage.loyalty__loyalty__extended_attributes_cur where sys_change_operation = 'D' limit 2);

drop table null.mv_to_service_ci_keys_from_ea on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_ea on cluster basic to service.ci_keys as
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
-- from (select * from stage.loyalty__loyalty__extended_attributes_cur where sys_change_operation <> 'D' limit 2);

--====> BO
drop table null.mv_to_service_ci_from_bo on cluster basic;
create materialized view null.mv_to_service_ci_from_bo on cluster basic to service.ci as
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
-- from (select * from stage.loyalty__loyalty__bonus_cur limit 2 );

drop table null.mv_to_service_ci_keys_from_bo on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_bo on cluster basic to service.ci_keys as
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
-- from (select * from stage.loyalty__loyalty__bonus_cur where sys_change_operation = 'D' limit 2);

--====> BW
drop table if exists null.mv_to_service_ci_from_bw on cluster basic;
create materialized view null.mv_to_service_ci_from_bw on cluster basic to service.ci as
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
-- from (select * from stage.loyalty__loyalty__bonus_wo_cur where (sys_change_operation = 'D' or cheque_id is not null) limit 2);

drop table null.mv_to_service_ci_keys_from_bw on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_bw on cluster basic to service.ci_keys as
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
-- from (select * from stage.loyalty__loyalty__bonus_wo_cur where sys_change_operation = 'D' limit 2);


--====> PAYMENT
drop table null.mv_to_service_ci_from_payment on cluster basic;
create materialized view null.mv_to_service_ci_from_payment on cluster basic to service.ci as
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
-- from stage.loyalty__loyalty__payment_cur limit 10;

drop table null.mv_to_service_ci_keys_from_payment on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_payment on cluster basic to service.ci_keys as
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
-- from stage.loyalty__loyalty__payment_cur where sys_change_operation = 'D' limit 2;


--====> COUPON
drop table if exists null.mv_to_service_ci_from_coupon on cluster basic;
create materialized view null.mv_to_service_ci_from_coupon on cluster basic to service.ci as
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
-- from stage.loyalty__loyalty__coupon_cur limit 10;

drop table null.mv_to_service_ci_keys_from_coupon on cluster basic;
create materialized view null.mv_to_service_ci_keys_from_coupon on cluster basic to service.ci_keys as
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
-- from stage.loyalty__loyalty__coupon_cur where sys_change_operation = 'D' limit 2;


CREATE TABLE service.mart_ci on cluster basic
(
    `instance_id` UInt16,
    `ch_number` String,
    `dt` Int64,
    `d` Date MATERIALIZED toDate(dt, 'UTC'),
    `ym` UInt32,
    `oper_type` Int32,
    `article_id` Int32,
    `article_hash` UInt64 MATERIALIZED cityHash64(article_id, instance_id, 1),
    `price` Int64,
    `quantity` Int64,
    `summdisc` Int64,
    `summ` Int64,
    `discount` Int64,
    `cheque_id` Int64,
    `chequeitem_id` Int64,
    `card_id` Int32,
    `card_hash` UInt64,
    `card_instance_hash` UInt64 MATERIALIZED cityHash64(card_id, instance_id),
    `coupon_num` Array(String),
    `ea_ch` Array(Tuple(String, String)),
    `ea_ci` Array(Tuple(String, String, Decimal(9, 2), Decimal(9, 2), String)),
    `campaign_id` Array(Int32),
    `bo_ch` Array(Tuple(Int64, Int32, Int32, Int64, UInt8, Enum8('cheque_item' = 1, 'cheque' = 2, 'extended_attributes' = 3, 'payment' = 4, 'bonus_wo' = 5, 'bonus' = 6, 'coupon' = 7, 'money' = 8, 'money_wo' = 9, 'card' = 10, 'money_transaction' = 11))),
    `bo_ci` Array(Tuple(Int64, Int32, Int32, Int64, UInt8, Enum8('cheque_item' = 1, 'cheque' = 2, 'extended_attributes' = 3, 'payment' = 4, 'bonus_wo' = 5, 'bonus' = 6, 'coupon' = 7, 'money' = 8, 'money_wo' = 9, 'card' = 10, 'money_transaction' = 11))),
    `shop_id` Int32,
    `organization_id` Int32,
    `pos_id` Int32,
    `summ_ch` Int64,
    `summdisc_ch` Int64,
    `is_del` UInt8,
    `contact_id` Int32,
    `partition_id` Int64 MATERIALIZED intDiv(chequeitem_id, 303030304),
    `dt_load` DateTime DEFAULT now(),
    `d_load` Date DEFAULT toDate(dt_load, 'UTC'),
    `start_of_month` Date MATERIALIZED toStartOfMonth(toDate(dt, 'UTC')),
    `start_of_week_from_thursday` Date MATERIALIZED if(toDayOfWeek(toDate(dt, 'UTC') AS dd) < 4, toStartOfWeek(dd, 1) - toIntervalDay(4), toStartOfWeek(dd, 1) + toIntervalDay(3)),
    `start_of_week` Date MATERIALIZED toStartOfWeek(toDate(dt, 'UTC'), 1),
    `organization_instance_hash` UInt64 MATERIALIZED cityHash64(organization_id, instance_id),
    `tenant_id` UInt16,
    `contact_instance_hash_calc` UInt64 ALIAS dictGet('dwh.d_card', 'contact_instance_hash', card_hash),
    `contact_hash_calc` UInt64 ALIAS dictGet('dwh.d_card', 'contact_hash', card_hash) ,
    `oper_type_name` LowCardinality(String) MATERIALIZED multiIf(oper_type = 1, 'Продажа', oper_type = 2, 'Возврат', oper_type = 3, 'Обмен', 'N/A'),
    `position_number` String,
    `paid_by_bonus` Int64,
    `mcp` Int64,
    `payment_type` Array(Tuple(Int32, Int64)) ,
    `card_type_id` Int32,
    `dt_insert` DateTime MATERIALIZED now(),
    `article_external_id` String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/service_mart_ci', '{replica}')
PARTITION BY (tenant_id, toYYYYMM(d))
ORDER BY (tenant_id, chequeitem_id)
SETTINGS index_granularity = 8192;

--определяем максимальную дату
with
    toDateTime('__pb__') as pb
    , least(now(), pb + interval __interval__ minute) as pe
select max(dt_load) as maxdt
from service.ci_keys
where dt_load between pb and pe;

-- truncate table service.mart_ci;
-- insert into service.mart_ci
insert into dwh.chequeitems_daily
with
    toDateTime('2024-07-04 11:00:00') as pb
    , toDateTime('2024-07-04 12:00:00') as pe
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
            select distinct arrayJoin([key_hash, related_hash])
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
    , greater(ch.ch.1, ci.ci.2) as is_del
    , (dictGet('dwh.d_card', tuple('contact_id', 'card_type_id'), card_hash) as card_attrs).1 AS contact_id
    , pe as dt_load
    , toDate(dt_load) as d_load
    , get_partner(0, instance_id, organization_id, 0) as tenant_id
    , ci.ci.9 as position_number
    , ch.ch.10 as paid_by_bonus
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

insert into service.ci_log
with
    toDateTime('2024-06-16 00:00:00') as pb
    , toDateTime('2024-06-16 01:00:00') as pe
    , dt_load between pb and pe as dt_where
select
    key_hash
    , attribute_hash
    , pb
    , pe
from service.ci_keys
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
                )
            )
        )
    )
);


--===== Логи загрузки =========

select
    dt_load
    , count()
from service.mart_ci
group by dt_load
order by dt_load desc;

select event_time, intDiv(query_duration_ms, 60000) as min
     , formatReadableQuantity(written_rows), query
from system.query_log
where event_date = today()
    and type = 'QueryFinish'
    and query like 'insert into service.mart_ci%'
order by event_time desc;

--======= Схождение данных =====

with (toDate('2024-06-28')) as period
select
    sou, count(), any(key)
from
(
    select
        key
        , any(source) as sou
    from
    (
        select (chequeitem_id, instance_id) as key, cityHash64(article_id, dt, is_del, shop_id) as attr, 1 as source from service.mart_ci where d = period
        union all
        select (chequeitem_id, instance_id) as key, cityHash64(article_id, dt, is_del, shop_id) as attr, 2 as source from dwh.chequeitems_retro where d = period
    ) group by key
    having count() = 1
) group by sou;


select
    sou, count(), any(key)
from
(
    select
        key
        , any(source) as sou
    from
    (
        with
            (cityHash64(article_id, is_del)) as attr
            , (toDate('2024-06-28')) as period
        select (chequeitem_id, instance_id) as key, attr, 1 as source from service.mart_ci where d = period
        union all
        select (chequeitem_id, instance_id) as key, attr, 2 as source from dwh.chequeitems_retro where d = period
    ) group by key
    having count() = 2 and uniqExact(attr) = 2
) group by sou;


--==== Прлизводительность запросов


select
    intDivOrZero(anyIf(mem, m = 'new method')*100, anyIf(mem, m = 'old method')) as percent_memory
    , intDivOrZero(anyIf(cpu, m = 'new method')*100, anyIf(cpu, m = 'old method')) as percent_cpu
from
(
    with (toDate('2024-06-28')) as period
    select
        sum(memory_usage) as mem
        , sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) as cpu
        , 'old method' as m
    from system.query_log
    where event_date = period
        and type = 'QueryFinish'
        and user = 'airflow_user'
        and
        (
            query like '%insert into dwh.chequeitems_daily%'
            or query like '%insert into  stage.ci_temporary_ch_new%'
            or query like '%insert into stage.ci_temporary_new%'
            or query like '%insert into stage.ci_temporary_group_ci%'
            or query like '%insert into stage.ci_temporary_group_c%'
            or query like '%insert into stage.ci_temporary_last_chequeitem_instance_hash%'
            or query like 'insert into chequeitems_retro%'
        )
    union all
    select
        sum(memory_usage)
        , sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])
        , 'new method' as m
    from system.query_log
    where event_date = period
        and type = 'QueryFinish'
        and user = 'airflow_user'
        and
        (
            query like '%insert into service.ci_log%'
            or query like '%insert into service.mart_ci%'
            or query like '%select max(dt_load) as maxdt%'
        )
);

--===== необходимость дедубликации

with (toDate('2024-06-29')) as period
select
    uniqExact((chequeitem_id, tenant_id)) as uniq_qty
    , count() as qty
    , intDiv(uniq_qty*100, qty) as perc_qty
    , qty - uniq_qty as abs_qty
from service.mart_ci
where d = period;

with (toDate('2024-06-25')) as period
select
    chequeitem_id
    , tenant_id
    , count()
    , groupArray(tuple(dt_load, arraySort(ea_ch), arraySort(ea_ci), arraySort(bo_ch), arraySort(bo_ci), * except (ea_ch, ea_ci, bo_ch, bo_ci)))
from service.mart_ci
where d = period
group by chequeitem_id, tenant_id
having count() > 1
order by count() desc;


select count(), uniqExact(partition)
from system.parts
where database = 'dwh'
    and table = 'bonus_slim_retro'
    and active