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
SETTINGS check_table_dependencies = 0;

show create dwh.rule