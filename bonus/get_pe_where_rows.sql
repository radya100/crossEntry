with
    toDateTime('__pb__') as pb
    , __rows__ as rows
select
    greatest((groupArray((dt_load, q)) as w)[arrayLastIndex(y -> y < rows, arrayCumSum(x -> x.2, w)) as i].1, pb) as maxdt
from
(
    select
        dt_load, count() as q
    from stage.bo_keys
    where dt_load > pb
    group by dt_load
    order by dt_load
    limit rows/10
)