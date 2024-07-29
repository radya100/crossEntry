create table stage.bo on cluster basic
(
    key_hash UInt128
    , instance_id UInt16
    , source_table UInt8
    , is_header UInt8
    , is_del	UInt8
    , last_version Int64 codec(DoubleDelta, ZSTD(3))
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
engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_bo', '{replica}', last_version)
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;


create table stage.bo_keys on cluster basic
(
    key_hash UInt128
    , related_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_bo_keys', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

create table stage.bo_log on cluster basic
(
    key_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime
    , pe DateTime
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_bo_log', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);



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
-- from null.loyalty__null__loyalty__bonus_cur;
from stage.loyalty__loyalty__bonus_cur limit 100;