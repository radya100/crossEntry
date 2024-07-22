drop table service.join_pbb_del_me  on cluster basic;
drop table service.set_pbb_del_me on cluster basic;

create table service.join_pbb_del_me  on cluster basic
(
    instance_id UInt16
    , chequeitem_id Int64
    , pbb Int64
) engine = Join(ANY, LEFT, instance_id, chequeitem_id);

create table service.set_pbb_del_me on cluster basic
(
    instance_id UInt16
    , chequeitem_id Int64
) engine = Set();

insert into service.join_pbb_del_me
select
    instance_id
    , chequeitem_id
    , argMax(paid_by_bonus, last_version) as pbb
from stage.stage_ci
prewhere d_load between '2024-06-01' and '2024-06-30'
where source_table = 1
group by instance_id, chequeitem_id
having pbb <> 0;

insert into service.set_pbb_del_me
select
    instance_id
    , chequeitem_id
from stage.stage_ci
prewhere d_load between '2024-06-01' and '2024-06-30'
where source_table = 1
group by instance_id, chequeitem_id
having argMax(paid_by_bonus, last_version) <> 0;

alter table dwh.chequeitems_daily  update
    paid_by_bonus = joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id)
-- where d = '2024-05-31'
where ym = 202406
    and (instance_id, chequeitem_id) in service.set_pbb_del_me;


alter table dwh.chequeitems_retro  update
    paid_by_bonus = joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id)
-- where d = '2024-05-31'
where ym = 202406;
--     and (instance_id, chequeitem_id) in service.set_pbb_del_me;


select * from system.mutations
where not is_done
-- order by create_time desc ;

select paid_by_bonus,  joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id) as pbb
from dwh.chequeitems_retro
where (instance_id, chequeitem_id) not in service.set_pbb_del_me
    and d = '2024-05-31'
    and paid_by_bonus <> pbb
order by paid_by_bonus desc
;

select
    d
    , formatReadableQuantity(sum(paid_by_bonus)/100) as pbb1
    , formatReadableQuantity(sum(joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id))/100) as pbb2
from dwh.chequeitems_retro
where (ym = 202406 or d = '2024-05-31')
    and tenant_id in (1)
--     and (instance_id, chequeitem_id) in service.set_pbb_del_me
group by d
order by d desc;



select * from system.zookeeper where path = '/clickhouse/tables/buran/chequeitems_daily_not_mat_tenant/mutations';
select * from system.zookeeper where path = '/clickhouse/tables/buran/chequeitems_retro_not_mat_tenant/mutations';



DETACH TABLE dwh.chequeitems_retro;  -- Required for DROP REPLICA
-- Use the zookeeper_path and replica_name from the above query.
SYSTEM DROP REPLICA '01' FROM ZKPATH '/clickhouse/tables/kim/chequeitems_retro_not_mat_tenant'; -- It will remove everything from the /table_path_in_zk/replicas/replica_name
ATTACH TABLE dwh.chequeitems_retro;  -- Table will be in readonly mode, because there is no metadata in ZK and after that execute
SYSTEM RESTORE REPLICA dwh.chequeitems_retro;  -- It will detach all partitions, re-create metadata in ZK (like it's new empty table), and then attach all partitions back
SYSTEM SYNC REPLICA dwh.chequeitems_retro; -- Wait for replicas to synchronize parts. Also it's recommended to check `system.detached_parts` on all replicas after recovery is finished.



