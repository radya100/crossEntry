insert into service.ci_log
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load between pb and pe as dt_where
select
    key_hash
    , attribute_hash
    , pb
    , pe
from service.ci_keys
where key_hash in
(
    select arrayJoin([key_hash, related_hash])
    from service.ci_keys
    where key_hash in
    (
        select arrayJoin([key_hash, related_hash])
        from service.ci_keys
        where key_hash in
        (
            select
                arrayJoin([key_hash, related_hash])
            from service.ci_keys
            where key_hash in
            (
                select
                    arrayJoin([key_hash, related_hash])
                from service.ci_keys
                where key_hash in
                (
                    select
                        arrayJoin([key_hash, related_hash])
                    from service.ci_keys
                    where dt_where
                )
            )
        )
    )
)