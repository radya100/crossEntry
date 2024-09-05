--===Сходимость по кол-ву записей и сумме скидки
with (toDate(dt_created) = '2024-09-01') as d_where
select
    tenant_id
    , anyIf(qty, sou = 'new') as new
    , anyIf(qty, sou = 'old') as old
    , anyIf(val, sou = 'new') as new_val
    , anyIf(val, sou = 'old') as old_val
from
(
    select
        tenant_id
        , uniqExact(b_instance_hash) as qty
        , sum(value) as val
        , 'new' as sou
    from service.qwe
    where d_where
    group by tenant_id
    union all
    select
        tenant_id
        , uniqExact(b_instance_hash) as qty
        , sum(value) as val
        , 'old' as sou
    from dwh.bonus_slim_retro
    where d_where
    group by tenant_id
) group by tenant_id
order by tenant_id;

--== Результат работы скриптов - с временем выполнения полного цикла загрузки
select
    (arrayJoin(arrayMap(x -> (ar[x].1, ar[x].2, ar[x].3, ar[x].4, ar[x].4 - ar[x+1].4, ar[x].5), arrayEnumerate(groupArray(t) as ar))) as aj).1 as pb
    , aj.2 as pe
    , aj.3 as type
    , aj.4 as time
    , formatReadableTimeDelta(aj.5) as sec
    , formatReadableQuantity(aj.6) as rows
from
(
    select tuple
        (
            substring((splitByString('toDateTime', query) as q)[2], 3, 19) as pb
            , substring(q[3], 3, 19) as pe
            , type
            , event_time
            , written_rows
        ) as t
    from system.query_log
    where event_date = today()
        and user = 'airflow_user'
        and type <> 'QueryStart'
        and http_user_agent = 'curl/7.64.0'
        and hasAny(['stage.set_bo'], tables) and query_kind = 'Create'
    order by event_time_microseconds desc
);


--==== Кверилог выполнения
select formatReadableQuantity(written_rows) as rows
    , type
    , event_time
    , query
    , * except ( result_bytes, type, event_time, query )
from system.query_log
where event_date = today()
    and user = 'airflow_user'
    and type <> 'QueryStart'
    and http_user_agent = 'curl/7.64.0'
    and hasAny(['stage.bo_keys', 'stage.bo_log', 'stage.set_bo','service.qwe', 'stage.bo', 'stage.bo_values'], tables)
order by event_time_microseconds desc;

--=== Штука , которая позволяет оценить необходимость дедубликации
select
    ym
    , formatReadableQuantity(count())
    , formatReadableQuantity(uniq((key_hash, attribute_hash, ym)))
from stage.bo_log
group by ym order by ym desc;
optimize table stage.bo_log partition 202409 final deduplicate by key_hash,attribute_hash, ym;

--=== Сколько входных данных было обработано
select pb, pe, formatReadableQuantity(count())
from stage.bo_log
group by pb, pe
order by pb desc;

select count()
from stage.bo
where key_hash in stage.set_bo;


show processlist;