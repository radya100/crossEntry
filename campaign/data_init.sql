insert into stage.campaign
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(CampaignId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 1 as is_header
    , sys_change_operation = 'D' as is_del
    , last_version
--campaign
    , assumeNotNull(CampaignId) as campaign_id
    , assumeNotNull(Name) as campaign_name
    , assumeNotNull(ActualStart) as actual_start
    , assumeNotNull(ActualEnd) as actual_end
    , assumeNotNull(externalid) as external_id
    , assumeNotNull(IsActive) as is_active
    , assumeNotNull(OwnerId) as owner_id
from stage.loyalty__crmdata__Campaign;

insert into stage.campaign_keys
with 2 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(CampaignId) as key_hash
select
    key_hash
    , key_hash as related_hash
    , cityHash64
        (
            assumeNotNull(Name)
            , assumeNotNull(ActualStart)
            , assumeNotNull(ActualEnd)
            , assumeNotNull(externalid)
            , assumeNotNull(IsActive)
            , assumeNotNull(OwnerId)
        ) as attribute_hash
    , source_table
from stage.loyalty__crmdata__Campaign;
