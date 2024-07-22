drop  table stage.pcvalue_ce on cluster basic
create table stage.pcvalue_ce on cluster basic
(
    key_hash UInt64--
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
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_pcvalue_ce_2', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash, is_del)
primary key key_hash;

drop table stage.pcvalue_ce_keys on cluster basic;
create table stage.pcvalue_ce_keys on cluster basic
(
    key_hash UInt64
    , related_hash UInt64
    , attribute_hash UInt64 codec(ZSTD(3))
    , source_table UInt8 codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_pcvalue_ce_keys_2', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

drop table stage.pcvalue_ce_log on cluster basic;
create table stage.pcvalue_ce_log on cluster basic
(
    key_hash UInt64
    , attribute_hash UInt64 codec(ZSTD(3))
    , pb DateTime codec(DoubleDelta, ZSTD(3))
    , pe DateTime codec(DoubleDelta, ZSTD(3))
    , dt_load DateTime materialized now()
) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/stage_pcvalue_ce_log_2', '{replica}')
partition by toYYYYMM(dt_load)
order by (key_hash);

-->> PCVALUE_CUR
drop table null.mv_to_stage_pcvalue_ce_from_pcvalue on cluster basic;
create materialized view null.mv_to_stage_pcvalue_ce_from_pcvalue on cluster basic to stage.pcvalue_ce as
with 7 as source_table
    , cityHash64(
        assumeNotNull(external_id)
        , assumeNotNull(contact_id)
        , assumeNotNull(card_id)
        , assumeNotNull(description)
        , tenant_id
        , source_table
    ) as key_hash
select
    key_hash
    , 0 as instance_id
    , source_table
    , 1 as is_header
    , is_del
    , reinterpretAsInt64(now()) as last_version

    , tenant_id
    , assumeNotNull(external_id) as external_id
    , assumeNotNull(contact_id) as contact_id
    , assumeNotNull(card_id) as card_id
    , assumeNotNull(product_iconurl) as product_iconurl
    , assumeNotNull(value) as value
    , assumeNotNull(effectivefrom) as effectivefrom
    , assumeNotNull(effectiveto) as effectiveto
    , assumeNotNull(message) as message
    , assumeNotNull(description) as description
    , assumeNotNull(condition_email) as condition_email
    , assumeNotNull(discount) as discount
    , assumeNotNull(red_pice) as red_pice
    , assumeNotNull(black_pice) as black_pice
    , assumeNotNull(product_name_for_conclusion) as product_name_for_conclusion
    , assumeNotNull(priority) as priority
    , assumeNotNull(group_id) as group_id
    , assumeNotNull(region_name) as region_name
from null.null_pcvalue;


drop table null.mv_to_stage_pcvalue_ce_keys_from_pcvalue on cluster basic;
create materialized view null.mv_to_stage_pcvalue_ce_keys_from_pcvalue on cluster basic to stage.pcvalue_ce_keys as
with 7 as source_table
    , cityHash64(
        assumeNotNull(external_id)
        , assumeNotNull(contact_id)
        , assumeNotNull(card_id)
        , assumeNotNull(description)
        , tenant_id
        , source_table
    ) as key_hash
select
    key_hash
    , key_hash as related_hash
    , cityHash64
        (
            assumeNotNull(external_id)
            , assumeNotNull(contact_id)
            , assumeNotNull(card_id)
            , assumeNotNull(product_iconurl)
            , assumeNotNull(value)
            , assumeNotNull(effectivefrom)
            , assumeNotNull(effectiveto)
            , assumeNotNull(message)
            , assumeNotNull(description)
            , assumeNotNull(condition_email)
            , assumeNotNull(discount)
            , assumeNotNull(red_pice)
            , assumeNotNull(black_pice)
            , assumeNotNull(product_name_for_conclusion)
            , assumeNotNull(priority)
            , assumeNotNull(group_id)
            , assumeNotNull(region_name)
        ) as attribute_hash
    , source_table
from null.null_pcvalue;
