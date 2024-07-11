--==> RULE 1
insert into stage.rule
(
    `key_hash` ,
    `instance_id` ,
    `source_table` ,
    `is_header` ,
    `is_del` ,
    `last_version` ,
    `rule_id` ,
    `rule_name` ,
    `date_from` ,
    `date_to` ,
    `owner_id` ,
    `is_active` ,
    `bonus_type` ,
    `use_commodity_campaign` ,
    `use_personal_campaign` ,
    `use_certificate` ,
    `use_articleset` ,
    `external_id` ,
    campaign_id
)
with 1 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(RuleId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 1 as is_header
    , sys_change_operation = 'D' as is_del
    , last_version
--rule
    , assumeNotNull(RuleId) as rule_id
    , assumeNotNull(name) as rule_name
    , toDate(assumeNotNull(datefrom)) as date_from
    , toDate(assumeNotNull(dateto)) as date_to
    , assumeNotNull(OwnerId) as owner_id
    , assumeNotNull(IsActive) as is_active
    , assumeNotNull(bonustype) as bonus_type
    , assumeNotNull(useCommodityCampaign) as use_commodity_campaign
    , assumeNotNull(usePersonalCampaign) as use_personal_campaign
    , assumeNotNull(usecertificate) as use_certificate
    , assumeNotNull(usearticleset) as use_articleset
    , assumeNotNull(externalid) as external_id
--campaign
    , assumeNotNull(campaign) as campaign_id
from stage.loyalty__crmdata__Rule;

insert into stage.rule_keys (`key_hash` ,`related_hash` ,`attribute_hash` ,`source_table`)
with 1 as source_table
    , 2 as related_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(RuleId) as init_key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * related_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(campaign) as init_related_key
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(init_key, init_related_key)]
                : [(init_key, init_related_key), (init_related_key, init_key)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , cityHash64
        (
            assumeNotNull(RuleId)
            , assumeNotNull(name)
            , toDate(assumeNotNull(datefrom))
            , toDate(assumeNotNull(dateto))
            , assumeNotNull(OwnerId)
            , assumeNotNull(IsActive)
            , assumeNotNull(bonustype)
            , assumeNotNull(useCommodityCampaign)
            , assumeNotNull(usePersonalCampaign)
            , assumeNotNull(usecertificate)
            , assumeNotNull(usearticleset)
            , assumeNotNull(externalid)
        ) as attribute_hash
    , source_table
-- from null.loyalty__null__crmdata__Rule;
from stage.loyalty__crmdata__Rule;

--===> CAMPAIGN 2
insert into stage.rule (
    `key_hash` ,
    `instance_id` ,
    `source_table` ,
    `is_header` ,
    `is_del` ,
    `last_version` ,
    `campaign_id` ,
    `campaign_name`
)
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
from stage.loyalty__crmdata__Campaign;

insert into stage.rule_keys (
    `key_hash` ,
    `related_hash` ,
    `attribute_hash` ,
    `source_table`
)
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
        ) as attribute_hash
    , source_table
from stage.loyalty__crmdata__Campaign;

--===> CHEQUESET_RULE_I 3
insert into stage.rule (
    `key_hash` ,
    `instance_id` ,
    `source_table` ,
    `is_header` ,
    `last_version` ,
    `is_del` ,
    `rule_id` ,
    `chequeset_id`
)
with 3 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeset_rule_iId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_header
    , last_version
    , sys_change_operation = 'D' as is_del
    , assumeNotNull(ruleid) as rule_id
    , assumeNotNull(chequesetid) as chequeset_id
from stage.loyalty__crmdata__chequeset_rule_i;

insert into stage.rule_keys (
    `key_hash` ,
    `related_hash` ,
    `source_table` ,
    `attribute_hash`
)
with 3 as src
    , 1 as dst
    , 4 as dst_1
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * src)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeset_rule_iId) as key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * dst)
        + toUInt128(9223372036854775808)
        + assumeNotNull(ruleid) as key_related
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * dst_1)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequesetid) as key_related_1
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(key, key, src)]
                : [(key, key_related, src), (key_related, key, dst), (key, key_related_1, src), (key_related_1, key, dst_1)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , tup.3 as source_table
    , cityHash64(assumeNotNull(ruleid)) as attribute_hash
from stage.loyalty__crmdata__chequeset_rule_i;

--===> CHEQUESET 4
insert into stage.rule (
    `key_hash` ,
    `instance_id` ,
    `source_table` ,
    `is_header` ,
    `last_version` ,
    `is_del` ,
    `chequeset_id` ,
    `use_orgunit`
)
with 4 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequesetId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_header
    , last_version
    , sys_change_operation = 'D' as is_del

    , assumeNotNull(chequesetId) as chequeset_id
    , assumeNotNull(useorgunit) as use_orgunit
from stage.loyalty__crmdata__chequeset;

insert into stage.rule_keys (
    `key_hash` ,
    `related_hash` ,
    `attribute_hash` ,
    `source_table`
)
with 4 as src
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * src)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequesetId) as key_hash
select
    key_hash
    , key_hash as related_hash
    , cityHash64(assumeNotNull(useorgunit)) as attribute_hash
    , src as source_table
from stage.loyalty__crmdata__chequeset;

--===> CHEQUESET_ORGUNITLIS_I 5

insert into stage.rule (
    `key_hash` ,
    `instance_id` ,
    `source_table` ,
    `is_header` ,
    `last_version` ,
    `is_del` ,
    `chequeset_id` ,
    `orgunitlist_id`
)
with 5 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeset_orgunitlist_iId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_header
    , last_version
    , sys_change_operation = 'D' as is_del
    , assumeNotNull(chequesetid) as chequeset_id
    , assumeNotNull(orgunitlistid) as orgunitlist_id
from stage.loyalty__crmdata__chequeset_orgunitlist_i;


insert into stage.rule_keys (
    `key_hash` ,
    `related_hash` ,
    `attribute_hash` ,
    `source_table`
)
with 5 as src
    , 4 as dst
    , 6 as dst_1
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * src)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequeset_orgunitlist_iId) as key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * dst)
        + toUInt128(9223372036854775808)
        + assumeNotNull(chequesetid) as key_related
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * dst_1)
        + toUInt128(9223372036854775808)
        + assumeNotNull(orgunitlistid) as key_related_1
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(key, key, src)]
                : [(key, key_related, src), (key_related, key, dst), (key, key_related_1, src), (key_related_1, key, dst_1)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , tup.3 as source_table
    , cityHash64(
        assumeNotNull(chequesetid)
        , assumeNotNull(orgunitlistid)
    ) as attribute_hash
from stage.loyalty__crmdata__chequeset_orgunitlist_i;


--===> ORGUNITLIST_ORGUNIT 6
insert into stage.rule (
    `key_hash` ,
    `instance_id` ,
    `source_table` ,
    `is_header` ,
    `last_version` ,
    `is_del` ,
    `orgunitlist_id` ,
    `shop_id`
)
with 6 as source_table
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * source_table)
        + toUInt128(9223372036854775808)
        + assumeNotNull(orgunitlist_orgunitId) as key_hash
select
    key_hash
    , instance_id
    , source_table
    , 0 as is_header
    , last_version
    , sys_change_operation = 'D' as is_del
    , assumeNotNull(orgunitlistid) as orgunitlist_id
    , assumeNotNull(orgunitid) as shop_id
from stage.loyalty__crmdata__orgunitlist_orgunit;

insert into stage.rule_keys (
    `key_hash` ,
    `related_hash` ,
    `attribute_hash` ,
    `source_table`
)
with 6 as src
    , 5 as dst
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * src)
        + toUInt128(9223372036854775808)
        + assumeNotNull(orgunitlist_orgunitId) as key
    , (toUInt128(10000000000000000000000) * instance_id)
        + (toUInt128(100000000000000000000) * dst)
        + toUInt128(9223372036854775808)
        + assumeNotNull(orgunitlistid) as key_related
select
    (
        arrayJoin
        (
            sys_change_operation = 'D'
                ? [(key, key, src)]
                : [(key, key_related, src), (key_related, key, dst)]
        ) as tup
    ).1 as key_hash
    , tup.2 as related_hash
    , cityHash64(assumeNotNull(orgunitid)) as attribute_hash
    , tup.3 as source_table
from stage.loyalty__crmdata__orgunitlist_orgunit;