create table stage.pcvalue on cluster basic
(
    key_hash UInt128--
    , instance_id UInt16
    , source_table UInt8
    , is_header UInt8
    , is_del UInt8--
    , last_version Int64 codec(DoubleDelta, ZSTD(3))--
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))

    , tenant_id	UInt16 codec(DoubleDelta, ZSTD(3))
    , external_id	UInt64 codec(DoubleDelta, ZSTD(3))
    , contact_id	Int32 codec(DoubleDelta, ZSTD(3))
    , card_id	Int32 codec(DoubleDelta, ZSTD(3))
    , product_iconurl	String codec(ZSTD(3))
    , value	String codec(ZSTD(3))
    , effectivefrom	DateTime codec(DoubleDelta, ZSTD(3))
    , effectiveto	DateTime codec(DoubleDelta, ZSTD(3))
    , message	String codec(ZSTD(3))
    , description	String codec(ZSTD(3))
    , condition_email	String codec(ZSTD(3))
    , discount	Int64 codec(DoubleDelta, ZSTD(3))
    , red_pice	Int64 codec(DoubleDelta, ZSTD(3))
    , black_pice	Int64 codec(DoubleDelta, ZSTD(3))
    , product_name_for_conclusion	String
    , priority	UInt8 codec(DoubleDelta, ZSTD(3))
    , group_id	UInt8 codec(DoubleDelta, ZSTD(3))
    , region_name String codec(ZSTD(3))
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_pcvalue', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;

create table stage.pcvalue_keys on cluster basic
(
    key_hash UInt128
    , related_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_pcvalue_keys', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

create table stage.pcvalue_log on cluster basic
(
    key_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime codec(DoubleDelta, ZSTD(3))
    , pe DateTime codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_pcvalue_log', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

-->> PCVALUE_CUR
drop table null.mv_to_stage_pcvalue_from_pcvalue on cluster basic;
create materialized view null.mv_to_stage_pcvalue_from_pcvalue on cluster basic to stage.pcvalue as
with 7 as source_table
    , (toUInt128(10000000000000000000000) * tenant_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(CampaignId) as key_hash
    , cityHash64(external_id, assumeNotNull(contact_id), assumeNotNull(card_id), assumeNotNull(description), instance_id)
select
    key_hash
--     , instance_id
    , source_table
--     , 1 as is_header
    , is_del
    , last_version


from null.null_pcvalue;

drop table null.mv_to_stage_campaign_keys_from_campaign on cluster basic;
create materialized view null.mv_to_stage_campaign_keys_from_campaign on cluster basic to stage.campaign_keys as
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(CampaignId) as key_hash
select
    key_hash
    , key_hash as related_hash
    , cityHash64
        (
            assumeNotNull(Name)
            , assumeNotNull(ActualStart)
            , assumeNotNull(ActualEnd)
            , assumeNotNull(externalid)
            , assumeNotNull(IsActive)
            , assumeNotNull(OwnerId)
        ) as attribute_hash
    , source_table
from null.loyalty__null__crmdata__Campaign;