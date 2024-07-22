with
    toDateTime('__pb__') as pb
    , __rows__ as rows
select greatest(max(dt_load), toDateTime('__pb__')) as maxdt
from
(
    select
        dt_load
    from stage.pcvalue_ce_keys
    where dt_load > pb
    order by dt_load
    limit rows
);