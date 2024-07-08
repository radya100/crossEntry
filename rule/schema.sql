create table stage.rule on cluster basic
(
    key_hash UInt128 codec(DoubleDelta, ZSTD(3))
    , instance_id UInt16
    , source_table UInt8
    , is_cheque UInt8
    , is_del	UInt8
    , last_version Int64 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now() codec(DoubleDelta, ZSTD(3))
-- rule
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
-- campaign
    , campaign_id Int32 codec(DoubleDelta, ZSTD(3))
    , campaign_name String codec(ZSTD(3))
-- chequeset_rule_i
    --rule_id
    --chequeset_rule_iId
    --chequeset_id
-- chequeset
    , chequeset_id Int32 codec(DoubleDelta, ZSTD(3))
    , use_orgunit Int32 codec(DoubleDelta, ZSTD(3))
-- chequeset_orgunitlist_i
    --chequeset_orgunitlist_Id
    --chequesetid
    --orgunitlist_id
-- orgunitlist_orgunit
    --orgunitlist_orgunitId
    , orgunitlist_id Int32 codec(DoubleDelta, ZSTD(3))
    , shop_id Int32 codec(DoubleDelta, ZSTD(3))
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_rule', '{replica}', last_version)
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;