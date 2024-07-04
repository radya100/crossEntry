with
    toDateTime('__pb__') as pb
    , __rows__ as rows
select max(dt_load) as maxdt
from
(
    select
        dt_load
    from service.ci_keys
    where dt_load > pb
    order by dt_load
    limit rows
);