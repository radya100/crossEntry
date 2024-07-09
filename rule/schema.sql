drop table stage.rule on cluster basic;
create table stage.rule on cluster basic
(
    key_hash UInt128
    , instance_id UInt16
    , source_table UInt8
    , is_header UInt8
    , is_del	UInt8
    , last_version Int64 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))
-- rule 1
    , rule_id Int32 codec(DoubleDelta, ZSTD(3))
    , rule_name String
    , date_from Date codec(DoubleDelta, ZSTD(3))
    , date_to Date codec(DoubleDelta, ZSTD(3))
    , owner_id Int32 codec(DoubleDelta, ZSTD(3))
    , is_active UInt8
    , bonus_type Int32 codec(DoubleDelta, ZSTD(3))
    , use_commodity_campaign Int32 codec(DoubleDelta, ZSTD(3))
    , use_personal_campaign UInt8
    , use_certificate UInt8
    , use_articleset UInt8
    , external_id String codec(ZSTD(3))
    --campaign_id
-- campaign 2
    , campaign_id Int32 codec(DoubleDelta, ZSTD(3))
    , campaign_name String codec(ZSTD(3))
-- chequeset_rule_i 3
    --rule_id
    --chequeset_rule_iId
    --chequeset_id
-- chequeset 4
    , chequeset_id Int32 codec(DoubleDelta, ZSTD(3))
    , use_orgunit Int32 codec(DoubleDelta, ZSTD(3))
-- chequeset_orgunitlist_i 5
    --chequeset_orgunitlist_Id
    --chequesetid
    --orgunitlist_id
-- orgunitlist_orgunit 6
    --orgunitlist_orgunitId
    , orgunitlist_id Int32 codec(DoubleDelta, ZSTD(3))
    , shop_id Int32 codec(DoubleDelta, ZSTD(3))
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_rule', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;


create table stage.rule_keys on cluster basic
(
    key_hash UInt128
    , related_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_rule_keys', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

create table stage.rule_log on cluster basic
(
    key_hash UInt128
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime codec(DoubleDelta, ZSTD(3))
    , pe DateTime codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_rule_log', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

--==> RULE 1
drop table null.mv_to_stage_rule_from_rule on cluster basic;
create materialized view null.mv_to_stage_rule_from_rule on cluster basic to stage.rule as
with 1 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(RuleId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 1 as is_header
    , sys_change_operation = 'D' as is_del
    , last_version
--rule
    , assumeNotNull(RuleId) as rule_id
    , assumeNotNull(name) as rule_name
    , toDate(assumeNotNull(datefrom)) as date_from
    , toDate(assumeNotNull(dateto)) as date_to
    , assumeNotNull(OwnerId) as owner_id
    , assumeNotNull(IsActive) as is_active
    , assumeNotNull(bonustype) as bonus_type
    , assumeNotNull(useCommodityCampaign) as use_commodity_campaign
    , assumeNotNull(usePersonalCampaign) as use_personal_campaign
    , assumeNotNull(usecertificate) as use_certificate
    , assumeNotNull(usearticleset) as use_articleset
    , assumeNotNull(externalid) as external_id
--campaign
    , assumeNotNull(campaign) as campaign_id
from null.loyalty__null__crmdata__Rule;
-- from stage.loyalty__crmdata__Rule;

drop table if exists null.mv_to_stage_rule_keys_from_rule on cluster basic;
create materialized view null.mv_to_stage_rule_keys_from_rule on cluster basic to stage.rule_keys as
with 1 as source_table
    , 2 as related_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(RuleId) as init_key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * related_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(campaign) as init_related_key
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(init_key, init_related_key)]
                : [(init_key, init_related_key), (init_related_key, init_key)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
        (
            assumeNotNull(RuleId)
            , assumeNotNull(name)
            , toDate(assumeNotNull(datefrom))
            , toDate(assumeNotNull(dateto))
            , assumeNotNull(OwnerId)
            , assumeNotNull(IsActive)
            , assumeNotNull(bonustype)
            , assumeNotNull(useCommodityCampaign)
            , assumeNotNull(usePersonalCampaign)
            , assumeNotNull(usecertificate)
            , assumeNotNull(usearticleset)
            , assumeNotNull(externalid)
        ) as attribute_hash
    , source_table
from null.loyalty__null__crmdata__Rule;
-- from stage.loyalty__crmdata__Rule;

--===> CAMPAIGN 2
drop table null.mv_to_stage_rule_from_campaign on cluster basic;
create materialized view null.mv_to_stage_rule_from_campaign on cluster basic to stage.rule as
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
from null.loyalty__null__crmdata__Campaign;

drop table null.mv_to_stage_rule_keys_from_campaign on cluster basic;
create materialized view null.mv_to_stage_rule_keys_from_campaign on cluster basic to stage.rule_keys as
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
        ) as attribute_hash
    , source_table
from null.loyalty__null__crmdata__Campaign;

--===> CHEQUESET_RULE_I 3
drop table if exists null.mv_to_stage_rule_from_chequeset_rule_i on cluster basic;
create materialized view null.mv_to_stage_rule_from_chequeset_rule_i on cluster basic to stage.rule as
with 3 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeset_rule_iId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_header
    , last_version
    , sys_change_operation = 'D' as is_del
    , assumeNotNull(ruleid) as rule_id
    , assumeNotNull(chequesetid) as chequeset_id
from null.loyalty__null__crmdata__chequeset_rule_i;

drop table if exists null.mv_to_stage_rule_keys_from_chequeset_rule_i on cluster basic;
create materialized view null.mv_to_stage_rule_keys_from_chequeset_rule_i on cluster basic to stage.rule_keys as
with 3 as src
    , 1 as dst
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * src)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeset_rule_iId) as key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * dst)
        + toUInt128(9223372036854775808)
        + assumeNotNull(ruleid) as key_related
select
    (arrayJoin([(key, key_related, src), (key_related, key, dst)]) as tup).1 as key_hash
    , tup.2 as related_hash
    , tup.3 as source_table
    , cityHash64(assumeNotNull(ruleid)) as attribute_hash
from null.loyalty__null__crmdata__chequeset_rule_i;


--===> CHEQUESET 4
