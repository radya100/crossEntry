insert into dwh.campaign
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load > pb and dt_load <= pe as dt_where
    , rs as
    (
        select
            (argMaxIf(tuple(* except (key_hash, is_del, last_version), dt_load), last_version, not is_del) as tup).1 as instance_id
            , tup.2 as source_table
            , tup
            , argMax(is_del, last_version) as del

        from stage.campaign
        where key_hash in
        (
            select
                arrayJoin([key_hash, related_hash])
            from stage.campaign_keys
            where key_hash in
            (
                select
                    arrayJoin([key_hash, related_hash])
                from stage.campaign_keys
                where dt_where
                and (key_hash, attribute_hash) not in
                (
                    select key_hash, attribute_hash
                    from stage.campaign_log
                    where key_hash in (select key_hash from stage.campaign_keys where dt_where)
                )
            )
        ) group by key_hash
        having instance_id and (source_table = 1 or del = 0)
    )
select
    cityHash64(campaign_id, instance_id) as campaign_instance_hash
    , tup.4 as campaign_id
    , tup.5 as campaign_name
    , get_partner(0, instance_id, 0, tup.10) as tenant_id
    , tup.6 as actual_start
    , tup.7 as actual_end
    , tup.8 as external_id
    , tup.9 as is_active
    , tup.11 as dt_load
from rs
where source_table = 2;