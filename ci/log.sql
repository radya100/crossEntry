insert into service.ci_log
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
    , dt_load > pb and dt_load <= pe as dt_where
select
    key_hash
    , attribute_hash
    , pb
    , pe
from service.ci_keys
where dt_where;