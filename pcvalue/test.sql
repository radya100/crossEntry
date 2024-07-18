select *
from system.columns
where database = 'dwh'
    and table = 'pcvalue_retro'
    and default_kind not in ('MATERIALIZED', 'ALIAS')
order by position