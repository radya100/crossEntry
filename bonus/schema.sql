create table stage.bo on cluster basic
(
    key_hash UInt128 --
    , instance_id UInt16
    , source_table UInt8
    , is_header UInt8
    , is_del	UInt8 --
    , last_version Int64 codec(DoubleDelta, ZSTD(3)) --
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))
-->> bonus
    , bonus_id Int64 codec(DoubleDelta, ZSTD(3))
    , credit_bonus_id Int64 codec(DoubleDelta, ZSTD(3))
    , organization_id Int32 codec(DoubleDelta, ZSTD(3))
    , value Int64 codec(ZSTD(3))
    , dt_created DateTime('UTC') codec(DoubleDelta, ZSTD(3))
    , parent_type_id Int32 codec(DoubleDelta, ZSTD(3))
    , parent_id	Int64 codec(DoubleDelta, ZSTD(3))
    , rule_id Int32 codec(DoubleDelta, ZSTD(3))
    , campaign_id Int32 codec(DoubleDelta, ZSTD(3))
    , is_status	UInt8 codec(DoubleDelta, ZSTD(3))
    , oper_type	String codec(ZSTD(3))
    , dt_start_date	DateTime('UTC') codec(DoubleDelta, ZSTD(3))
    , dt_finish_date	DateTime('UTC') codec(DoubleDelta, ZSTD(3))
    , is_order	UInt8 codec(DoubleDelta, ZSTD(3))
    , remainder	Int64 codec(DoubleDelta, ZSTD(3))
-->> CI
    , chequeitem_id	Int64 codec(DoubleDelta, ZSTD(3))
    , article_id	Int32 codec(DoubleDelta, ZSTD(3))
    , ci_quantity	Int64 codec(ZSTD(3))
    , ci_summ	Int64 codec(ZSTD(3))
    , ci_summdiscounted	Int64 codec(ZSTD(3))
-->> EA_CI
    , ea_key String codec(ZSTD(3))
    , ea_value String codec(ZSTD(3))
-->> CH
    , cheque_id	Int64 codec(DoubleDelta, ZSTD(3))
    , shop_id	Int32 codec(DoubleDelta, ZSTD(3))
    , card_id	Int32 codec(DoubleDelta, ZSTD(3))
    , cheque_summ	Int64 codec(ZSTD(3))
    , cheque_summdiscounted	Int64 codec(ZSTD(3))
)
engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_bo', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;


create table stage.bo_keys on cluster basic
(
    key_hash UInt128
    , related_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))
    , ym Int32 materialized toYYYYMM(now()) codec(DoubleDelta, ZSTD(3))
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_bo_keys', '{replica}')
partition by ym
order by (key_hash);

create table stage.bo_log on cluster basic
(
    key_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime codec(DoubleDelta, ZSTD(3))
    , pe DateTime codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))
    , ym Int32 materialized toYYYYMM(now()) codec(DoubleDelta, ZSTD(3))
) engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/stage_bo_log_2', '{replica}')
partition by ym
order by (key_hash, attribute_hash)
primary key key_hash;


-->> BO
drop table if exists null.mv_to_stage_bo_from_bonus on cluster basic;
create materialized view null.mv_to_stage_bo_from_bonus on cluster basic to stage.bo as
with 6 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_id) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(bonus_id) as bonus_id
    , 0 as credit_bonus_id
    , assumeNotNull(organization_id) as organization_id
    , assumeNotNull(value) as value
    , assumeNotNull(created_on) as dt_created
    , assumeNotNull(parent_type_id) as parent_type_id
    , assumeNotNull(parent_id) as parent_id
    , assumeNotNull(rule_id) as rule_id
    , assumeNotNull(campaign_id) as campaign_id
    , assumeNotNull(isStatus) as is_status
    , assumeNotNull(operation_type_id) as oper_type
    , assumeNotNull(start_date) as dt_start_date
    , assumeNotNull(finish_date) as dt_finish_date
    , assumeNotNull(order_id) <> 0 as is_order
    , assumeNotNull(remainder) as remainder
    , assumeNotNull(cheque_item_id) as chequeitem_id
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(card_id) as card_id
from null.loyalty__null__loyalty__bonus_cur;
-- from stage.loyalty__loyalty__bonus_cur limit 100;

drop table if exists null.mv_to_stage_bo_keys_from_bonus on cluster basic;
create materialized view null.mv_to_stage_bo_keys_from_bonus on cluster basic to stage.bo_keys as
with  6 as source_table_bo
    , 1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_bo)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_id) as init_key_bo
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
            sys_change_operation = 'D', [(init_key_bo, init_key_bo, source_table_bo)]
            , assumeNotNull(cheque_id) =  0 and assumeNotNull(cheque_item_id) = 0, [(init_key_bo, init_key_bo, source_table_bo)]
            , assumeNotNull(cheque_id) <> 0 and assumeNotNull(cheque_item_id) = 0, [(init_key_bo, init_key_ch, source_table_bo), (init_key_ch, init_key_bo, source_table_ch)]
            , [(init_key_bo, init_key_ci, source_table_bo), (init_key_ci, init_key_bo, source_table_ci), (init_key_bo, init_key_ch, source_table_bo), (init_key_ch, init_key_bo, source_table_ch)]
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
from null.loyalty__null__loyalty__bonus_cur;
-- from stage.loyalty__loyalty__bonus_cur limit 100;


-->> BW
drop table if exists null.mv_to_stage_bo_from_bonus_wo on cluster basic;
create materialized view null.mv_to_stage_bo_from_bonus_wo on cluster basic to stage.bo as
with 5 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_wo_id) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(bonus_wo_id) as bonus_id
    , assumeNotNull(source_bonus_id) as credit_bonus_id
    , assumeNotNull(organization_id) as organization_id
    , assumeNotNull(value) as value
    , assumeNotNull(created_on) as dt_created
    , assumeNotNull(parent_type_id) as parent_type_id
    , assumeNotNull(parent_id) as parent_id
    , assumeNotNull(rule_id) as rule_id
    , assumeNotNull(campaign_id) as campaign_id
    , assumeNotNull(isStatus) as is_status
    , assumeNotNull(operation_type_id) as oper_type
    , assumeNotNull(start_date) as dt_start_date
    , assumeNotNull(finish_date) as dt_finish_date
    , assumeNotNull(order_id) <> 0 as is_order
    , assumeNotNull(remainder) as remainder
    , assumeNotNull(cheque_item_id) as chequeitem_id
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(card_id) as card_id
from null.loyalty__null__loyalty__bonus_wo_cur;
-- from stage.loyalty__loyalty__bonus_wo_cur limit 100;

drop table if exists null.mv_to_stage_bo_keys_from_bonus_wo on cluster basic;
create materialized view null.mv_to_stage_bo_keys_from_bonus_wo on cluster basic to stage.bo_keys as
with  5 as source_table_bo
    , 1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_bo)
        + toUInt128(9223372036854775808)
        + assumeNotNull(bonus_wo_id) as init_key_bo
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
            sys_change_operation = 'D', [(init_key_bo, init_key_bo, source_table_bo)]
            , assumeNotNull(cheque_id) =  0 and assumeNotNull(cheque_item_id) = 0, [(init_key_bo, init_key_bo, source_table_bo)]
            , assumeNotNull(cheque_id) <> 0 and assumeNotNull(cheque_item_id) = 0, [(init_key_bo, init_key_ch, source_table_bo), (init_key_ch, init_key_bo, source_table_ch)]
            , [(init_key_bo, init_key_ci, source_table_bo), (init_key_ci, init_key_bo, source_table_ci), (init_key_bo, init_key_ch, source_table_bo), (init_key_ch, init_key_bo, source_table_ch)]
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
from null.loyalty__null__loyalty__bonus_wo_cur;
-- from stage.loyalty__loyalty__bonus_wo_cur limit 100;

-->> EA
drop table if exists null.mv_to_stage_bo_from_ea on cluster basic;
create materialized view null.mv_to_stage_bo_from_ea on cluster basic to stage.bo as
with 3 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(extended_attributes_id) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(chequeitem_id) as chequeitem_id
    , assumeNotNull(key) as ea_key
    , assumeNotNull(value) as ea_value
from null.loyalty__null__loyalty__extended_attributes_cur;
-- from stage.loyalty__loyalty__extended_attributes_cur limit 100;

drop table if exists null.mv_to_stage_bo_keys_from_ea on cluster basic;
create materialized view null.mv_to_stage_bo_keys_from_ea on cluster basic to stage.bo_keys as
with  3 as source_table_ea
    , 1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ea)
        + toUInt128(9223372036854775808)
        + assumeNotNull(extended_attributes_id) as init_key_ea
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
            sys_change_operation = 'D', [(init_key_ea, init_key_ea, source_table_ea)]
            , assumeNotNull(cheque_id) =  0 and assumeNotNull(chequeitem_id) = 0, [(init_key_ea, init_key_ea, source_table_ea)]
            , assumeNotNull(cheque_id) <> 0 and assumeNotNull(chequeitem_id) = 0, [(init_key_ea, init_key_ch, source_table_ea), (init_key_ch, init_key_ea, source_table_ch)]
            , [(init_key_ea, init_key_ci, source_table_ea), (init_key_ci, init_key_ea, source_table_ci), (init_key_ea, init_key_ch, source_table_ea), (init_key_ch, init_key_ea, source_table_ch)]
        )
    ) as tup).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
    (
        assumeNotNull(cheque_id) as cheque_id
        , assumeNotNull(chequeitem_id) as chequeitem_id
        , assumeNotNull(key) as ea_key
        , assumeNotNull(value) as ea_value
    ) as attribute_hash
    , tup.3 as source_table
from null.loyalty__null__loyalty__extended_attributes_cur;
-- from stage.loyalty__loyalty__extended_attributes_cur limit 100;

--> CI
drop table if exists null.mv_to_stage_bo_from_ci on cluster basic;
create materialized view null.mv_to_stage_bo_from_ci on cluster basic to stage.bo as
with 1 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeitem_Id) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(chequeitem_Id) as chequeitem_id
    , assumeNotNull(cheque_id) as cheque_id
    , assumeNotNull(article_id) as article_id
    , assumeNotNull(quantity) as ci_quantity
    , assumeNotNull(summ) as ci_summ
    , assumeNotNull(summdiscounted) as ci_summdiscounted
from null.loyalty__null__loyalty__chequeitem_cur;
-- from stage.loyalty__loyalty__chequeitem_cur limit 100;

drop table if exists null.mv_to_stage_bo_keys_from_ci on cluster basic;
create materialized view null.mv_to_stage_bo_keys_from_ci on cluster basic to stage.bo_keys as
with  1 as source_table_ci
    , 2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ci)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeitem_Id) as init_key_ci
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_id) as init_key_ch
select
    (arrayJoin(
        multiIf(
            sys_change_operation = 'D', [(init_key_ci, init_key_ci, source_table_ci)]
            , [(init_key_ci, init_key_ch, source_table_ci), (init_key_ch, init_key_ci, source_table_ch)]
        )
    ) as tup).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
    (
        assumeNotNull(cheque_id) as cheque_id
        , assumeNotNull(article_id) as article_id
        , assumeNotNull(quantity) as ci_quantity
        , assumeNotNull(summ) as ci_summ
        , assumeNotNull(summdiscounted) as ci_summdiscounted
    ) as attribute_hash
    , tup.3 as source_table
from null.loyalty__null__loyalty__chequeitem_cur;
-- from stage.loyalty__loyalty__chequeitem_cur limit 100;

--> CH
drop table if exists null.mv_to_stage_bo_from_ch on cluster basic;
create materialized view null.mv_to_stage_bo_from_ch on cluster basic to stage.bo as
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_Id) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_cheque
    , sys_change_operation = 'D' as is_del
    , last_version

    , assumeNotNull(cheque_Id) as cheque_id
    , assumeNotNull(orgunit_id) as shop_id
    , assumeNotNull(card_id) as card_id
    , assumeNotNull(summ) as cheque_summ
    , assumeNotNull(summdiscounted) as cheque_summdiscounted
from null.loyalty__null__loyalty__cheque_cur;
-- from stage.loyalty__loyalty__cheque_cur limit 100;

drop table if exists null.mv_to_stage_bo_keys_from_ch on cluster basic;
create materialized view null.mv_to_stage_bo_keys_from_ch on cluster basic to stage.bo_keys as
with  2 as source_table_ch
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table_ch)
        + toUInt128(9223372036854775808)
        + assumeNotNull(cheque_Id) as init_key_ch
select
    init_key_ch as key_hash
    , init_key_ch as related_hash
    , cityHash64
    (
        assumeNotNull(orgunit_id) as shop_id
        , assumeNotNull(card_id) as card_id
        , assumeNotNull(summ) as cheque_summ
        , assumeNotNull(summdiscounted) as cheque_summdiscounted
    ) as attribute_hash
    , source_table_ch as source_table
from null.loyalty__null__loyalty__cheque_cur;
-- from stage.loyalty__loyalty__cheque_cur limit 100;


