--===== Логи загрузки =========

select
    dt_load
    , count()
from dwh.campaign
group by dt_load
order by dt_load desc;

select event_time
     , intDiv(query_duration_ms, 60000) as min
     , formatReadableQuantity(written_rows), query
from system.query_log
where event_date >= today()-1
    and type = 'QueryFinish'
    and query like 'insert into service.campaign_del_me%'
--     and written_rows > 0
order by event_time desc;

select * from stage.campaign_log;;

truncate table stage.campaign_log on cluster basic;
truncate table service.campaign_del_me on cluster basic;

drop table if exists dwh.campaign on cluster basic settings check_table_dependencies = 0;

system reload dictionary dwh.d_campaign on cluster basic;

select * from dwh.d_campaign;

CREATE VIEW dwh.campaign
(
    `campaign_instance_hash` UInt64,
    `campaign_id` Int32,
    `campaign_name` String,
    `tenant_id` UInt32,
    `actual_start` Int64,
    `actual_end` Int64,
    `external_id` String,
    `is_active` UInt8
) AS
SELECT
    cityHash64(campaign_id, instance_id) AS campaign_instance_hash,
    assumeNotNull(CampaignId) AS campaign_id,
    assumeNotNull(argMax(Name, last_version)) AS campaign_name,
    get_partner(0, instance_id, 0, assumeNotNull(argMax(OwnerId, last_version))) AS tenant_id,
    argMax(assumeNotNull(ActualStart), last_version) AS actual_start,
    argMax(assumeNotNull(ActualEnd), last_version) AS actual_end,
    argMax(assumeNotNull(externalid), last_version) AS external_id,
    argMax(assumeNotNull(IsActive), last_version) AS is_active
FROM stage.loyalty__crmdata__Campaign
GROUP BY
    campaign_id,
    instance_id
SETTINGS check_table_dependencies = 0
