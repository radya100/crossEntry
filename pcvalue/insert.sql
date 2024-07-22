insert into dwh.pcvalue_retro
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load between pb and pe as dt_where
    , rs as
    (
        select
            (argMaxIf(tuple(* except (is_del, last_version), dt_load), last_version, not is_del) as tup).5 as tenant_id
            , tup.3 as source_table
            , tup
            , argMax(is_del, last_version) as del
        from stage.pcvalue_ce
        where key_hash in
        (
            select
                arrayJoin([key_hash, related_hash])
            from stage.pcvalue_ce_keys
            where key_hash in
            (
                select
                    arrayJoin([key_hash, related_hash])
                from stage.pcvalue_ce_keys
                where dt_where
                and (key_hash, attribute_hash) not in
                (
                    select key_hash, attribute_hash
                    from stage.pcvalue_ce_log
                    where key_hash in (select key_hash from stage.pcvalue_ce_keys where dt_where)
                )
            )
        ) group by key_hash
        having tenant_id and (source_table = 7 or del = 0)
    )
select
    tup.1 as pcvalue_instance_hash
    , get_instance(tenant_id) as instance_id
    , tenant_id
    , tup.6 as external_id
    , cityHash64(external_id, instance_id) as external_instance_hash
    , tup.7 as contact_id
    , cityHash64(contact_id, instance_id) as contact_instance_hash
    , tup.8 as card_id
    , cityHash64(card_id, instance_id) as card_instance_hash
    , tup.9 as product_iconurl
    , tup.10 as value
    , tup.11 as effectivefrom
    , tup.12 as effectiveto
    , tup.13 as message
    , tup.14 as description
    , tup.15 as condition_email
    , tup.16 as discount
    , tup.17 as red_pice
    , tup.18 as black_pice
    , tup.19 as product_name_for_conclusion
    , tup.20 as priority
    , tup.21 as group_id
    , tup.22 as region_name
    , cityHash64(card_id, instance_id, get_salt(instance_id)) AS card_hash
    , cityHash64(contact_id, instance_id, get_salt(instance_id)) AS contact_hash
from rs
where source_table = 7;