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