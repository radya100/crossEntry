insert into stage.bo_log
with
    toDateTime('__pb__') as pb
    , toDateTime('__pe__') as pe
select
    key_hash
    , attribute_hash
    , pb
    , pe
from stage.bo_keys
where key_hash in stage.set_bo