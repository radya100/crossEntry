insert into stage.campaign_log
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load > pb and dt_load <= pe as dt_where
select
    key_hash
    , attribute_hash
    , pb
    , pe
from stage.campaign_keys
where key_hash in
(
    select arrayJoin([key_hash, related_hash])
    from stage.campaign_keys
    where key_hash in
    (
        select arrayJoin([key_hash, related_hash])
        from stage.campaign_keys
        where key_hash in
        (
            select
                arrayJoin([key_hash, related_hash])
            from stage.campaign_keys
            where dt_where
        )
    )
)