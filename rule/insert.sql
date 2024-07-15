insert into dwh.rule
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load between pb and pe as dt_where
    , rs as
    (
        select
            (argMaxIf(tuple(* except (key_hash, is_del, last_version), dt_load), last_version, not is_del) as tup).1 as instance_id
            , tup.2 as source_table
            , tup
            , argMax(is_del, last_version) as del

        from stage.rule
        where key_hash in
        (
            select arrayJoin([key_hash, related_hash])
            from stage.rule_keys
            where key_hash in
            (
                select arrayJoin([key_hash, related_hash])
                from stage.rule_keys
                where key_hash in
                (
                    select
                        arrayJoin([key_hash, related_hash])
                    from stage.rule_keys
                    where key_hash in
                    (
                        select
                            arrayJoin([key_hash, related_hash])
                        from stage.rule_keys
                        where key_hash in
                        (
                            select
                                arrayJoin([key_hash, related_hash])
                            from stage.rule_keys
                            where key_hash in
                            (
                                select
                                    arrayJoin([key_hash, related_hash])
                                from stage.rule_keys
                                where key_hash in
                                (
                                    select
                                        arrayJoin([key_hash, related_hash])
                                    from stage.rule_keys
                                    where dt_where
                                    and (key_hash, attribute_hash) not in
                                    (
                                        select key_hash, attribute_hash
                                        from stage.rule_log
                                        where key_hash in (select key_hash from stage.rule_keys where dt_where)
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ) group by key_hash
        having instance_id and (source_table = 1 or del = 0)
    )
select
    cityHash64(s1.rule_id, s1.instance_id) as rule_instance_hash
    , s1.rule_id
    , s1.instance_id
    , s1.rule_name
    , s1.campaign_id
    , cityHash64(s1.campaign_id, s1.instance_id) as campaign_instance_hash
    , s3.campaign_name
    , get_partner(0, s1.instance_id, 0, s1.owner_id) as tenant_id
    , s1.date_from
    , s1.date_to
    , s1.owner_id
    , s2.lists_shop_instance_hash
    , is_active
    , s1.bonus_type
    , s1.use_commodity_campaign
    , s1.use_personal_campaign
    , s1.use_certificate
    , s1.use_articleset
    , s1.external_id
    , greatest(s1.dt_load_, s2.dt_load_, s3.dt_load_) as dt_load
from
(
    select
        tup.4 as rule_id
        , tup.1 as instance_id
        , tup.5 as rule_name
        , tup.16 as campaign_id
        , tup.6 as date_from
        , tup.7 as date_to
        , tup.8 as owner_id
        , tup.9 as is_active
        , tup.10 as bonus_type
        , tup.11 as use_commodity_campaign
        , tup.12 as use_personal_campaign
        , tup.13 as use_certificate
        , tup.14 as use_articleset
        , tup.15 as external_id
        , tup.22 as dt_load_
    from rs
    where source_table = 1
) as s1
any left join
(
    select
        rule_id
        , a1.instance_id as instance_id
        , groupUniqArray(cityHash64(a3.shop_id, a1.instance_id)) as lists_shop_instance_hash
        , greatest(max(a1.dt_load_), max(a2.dt_load_), max(a3.dt_load_)) as dt_load_
    from
    (
        select
            tup.1 as instance_id
            , tup.4 as rule_id
            , tup.18 as chequeset_id
            , tup.22 as dt_load_
        from rs
        where source_table = 3
            and (chequeset_id, instance_id) in
            (
                select
                    tup.18 as chequeset_id
                    , tup.1 as instance_id
                from rs
                where source_table = 4
                    and tup.19 in (2, 4)
            )
    ) as a1
    all left join
    (
        select
            tup.1 as instance_id
            , tup.18 as chequeset_id
            , tup.20 as orgunitlist_id
            , tup.22 as dt_load_
        from rs
        where source_table = 5
    ) as a2 on (a2.chequeset_id, a2.instance_id) = (a1.chequeset_id, a1.instance_id)
    all left join
    (
        select
            tup.1 as instance_id
            , tup.20 as orgunitlist_id
            , tup.21 as shop_id
            , tup.22 as dt_load_
        from rs
        where source_table = 6
    ) as a3 on (a3.orgunitlist_id, a3.instance_id) = (a2.orgunitlist_id, a2.instance_id)
    group by rule_id, a1.instance_id
) as s2 on (s2.rule_id, s2.instance_id) = (s1.rule_id, s1.instance_id)
any left join
(
    select
        tup.1 as instance_id
        , tup.16 as campaign_id
        , tup.17 as campaign_name
        , tup.22 as dt_load_
    from rs
    where source_table = 2
) as s3 on (s3.campaign_id, s3.instance_id) = (s1.campaign_id, s1.instance_id);
