drop table service.join_pbb_del_me  on cluster basic;

create table service.join_pbb_del_me  on cluster basic
(
    instance_id UInt16
    , chequeitem_id Int64
    , pbb Int64
) engine = Join(ANY, LEFT, instance_id, chequeitem_id);

insert into service.join_pbb_del_me
select
    instance_id
    , chequeitem_id
    , argMax(paid_by_bonus, last_version) as pbb
from stage.stage_ci
prewhere d_load >= '2024-07-01'
where source_table = 1
group by instance_id, chequeitem_id
having pbb <> 0
;


alter table dwh.chequeitems_daily  update
    paid_by_bonus = joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id)
where ym = 202407 ;

alter table dwh.chequeitems_retro  update
    paid_by_bonus = joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id)
where ym = 202407 ;

-- create table service.pbb_del_me engine = Log as
select * except (pbb), pbb as paid_by_bonus
from
(
    select
        * except (paid_by_bonus)
        , joinGet('service.join_pbb_del_me', 'pbb', instance_id, chequeitem_id) as pbb
    from dwh.chequeitems_retro
    where ym = 202407
        and pbb <> paid_by_bonus
) where pbb <> 0 limit 100;


select * from system.mutations where not is_done;

select paid_by_bonus from service.ci where (chequeitem_id, instance_id) = (-9223372036537791365, 3) ;

select
    d
    , sum(paid_by_bonus)
from dwh.chequeitems_retro
where ym = 202407
    and tenant_id in (405, 406)
group by d
order by d desc;



select * from system.zookeeper where path = '/clickhouse/tables/{shard}/chequeitems_daily_not_mat_tenant/mutations';
select * from system.zookeeper where path = '/clickhouse/tables/{shard}/chequeitems_retro_not_mat_tenant/mutations';



DETACH TABLE dwh.chequeitems_retro;  -- Required for DROP REPLICA
-- Use the zookeeper_path and replica_name from the above query.
SYSTEM DROP REPLICA '01' FROM ZKPATH '/clickhouse/tables/kim/chequeitems_retro_not_mat_tenant'; -- It will remove everything from the /table_path_in_zk/replicas/replica_name
ATTACH TABLE dwh.chequeitems_retro;  -- Table will be in readonly mode, because there is no metadata in ZK and after that execute
SYSTEM RESTORE REPLICA dwh.chequeitems_retro;  -- It will detach all partitions, re-create metadata in ZK (like it's new empty table), and then attach all partitions back
SYSTEM SYNC REPLICA dwh.chequeitems_retro; -- Wait for replicas to synchronize parts. Also it's recommended to check `system.detached_parts` on all replicas after recovery is finished.



