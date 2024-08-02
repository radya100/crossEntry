create or replace table stage.set_bo engine = Set() as
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load between pb and pe as dt_where
select
    arrayJoin([key_hash, related_hash]) as key_hash
from stage.bo_keys
where key_hash in
(
    select
        arrayJoin([key_hash, related_hash])
    from stage.bo_keys
    where key_hash in
    (
        select
            arrayJoin([key_hash, related_hash])
        from stage.bo_keys
        where key_hash in
        (
            select
                arrayJoin([key_hash, related_hash])
            from stage.bo_keys
            where key_hash in
            (
                select
                    arrayJoin([key_hash, related_hash])
                from stage.bo_keys
                where key_hash in
                (
                    select arrayJoin([key_hash, related_hash])
                    from stage.bo_keys
                    where dt_where
                        and (key_hash, attribute_hash) not in
                        (
                            select key_hash, attribute_hash
                            from stage.bo_log
                            where key_hash in (select key_hash from stage.rule_keys where dt_where)
                        )
                )
            )
        )
    )
);