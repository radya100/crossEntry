create or replace table stage.bo_values engine = Log as
select
    (argMaxIf(tuple(* except (key_hash, is_del, last_version), dt_load), last_version, not is_del) as tup).1 as instance_id
    , tup.2 as source_table
    , tup
    , argMax(is_del, last_version) as del
from stage.bo
where key_hash in stage.set_bo
group by key_hash
having instance_id and (source_table in (5, 6) or del = 0);