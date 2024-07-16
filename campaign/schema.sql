-- create table dwh.campaign on cluster basic
create table service.campaign_del_me on cluster basic
(
    campaign_instance_hash UInt64
    , campaign_id Int32
    , campaign_name String
    , tenant_id UInt32
    , actual_start Int64
    , actual_end Int64
    , external_id String
    , is_active UInt8
    , dt_load DateTime default now()
) engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/service_campaign_del_me', '{replica}')
order by (tenant_id, campaign_instance_hash);

create table stage.campaign on cluster basic
(
    key_hash UInt128--
    , instance_id UInt16
    , source_table UInt8
    , is_header UInt8
    , is_del UInt8--
    , last_version Int64 codec(DoubleDelta, ZSTD(3))--
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))

    , campaign_id Int32 codec(DoubleDelta, ZSTD(3)) --5
    , campaign_name String codec(ZSTD(3))
    , actual_start Int64 codec(DoubleDelta, ZSTD(3))
    , actual_end Int64 codec(DoubleDelta, ZSTD(3))
    , external_id String codec(ZSTD(3))
    , is_active UInt8 codec(DoubleDelta, ZSTD(3))
    , owner_id Int32 codec(DoubleDelta, ZSTD(3))
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_campaign', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;

create table stage.campaign_keys on cluster basic
(
    key_hash UInt128
    , related_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_campaign_keys', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

create table stage.campaign_log on cluster basic
(
    key_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime codec(DoubleDelta, ZSTD(3))
    , pe DateTime codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_campaign_log', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);



--===> CAMPAIGN 2
drop table null.mv_to_stage_campaign_from_campaign on cluster basic;
create materialized view null.mv_to_stage_campaign_from_campaign on cluster basic to stage.campaign as
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(CampaignId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 1 as is_header
    , sys_change_operation = 'D' as is_del
    , last_version
--campaign
    , assumeNotNull(CampaignId) as campaign_id
    , assumeNotNull(Name) as campaign_name
    , assumeNotNull(ActualStart) as actual_start
    , assumeNotNull(ActualEnd) as actual_end
    , assumeNotNull(externalid) as external_id
    , assumeNotNull(IsActive) as is_active
    , assumeNotNull(OwnerId) as owner_id
from null.loyalty__null__crmdata__Campaign;

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

