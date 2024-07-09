select *
from stage.rule
where is_del = 0
limit 1 by source_table