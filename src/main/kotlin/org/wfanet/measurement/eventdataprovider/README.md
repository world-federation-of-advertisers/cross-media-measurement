# Event Data Provider (EDP) Guide

The code under this directory represents processes that happen at individual 
EDPs and thus are reference implementations. Any EDP can 
change the implementation. This code represents a good alternative and 
would work out of the box without any significant changes needed.

## Event Filtration

Event Filters are [CEL](https://github.com/google/cel-spec) expressions. 
The fields of these CEL expressions are defined in market specifics event templates ([examples](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/api/v2alpha/event_templates/testing)).
<br />This code provides validation and filtration support for these expressions. The recommended way to filter events is to map them to 
an [event proto](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/api/v2alpha/event_templates/testing/test_event.proto#L26) (note that this proto is just an example and in production, event proto will be market specific)
and simply [use the filtering function](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/test/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration/EventFiltersTest.kt#L46).

## Privacy Budget management (PBM)

The definition of PBM is EDP specific. This code assumes a definition based on [PrivacyBucketGroups (PBG)](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBucketGroup.kt).
<br />[The backing store](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBudgetLedgerBackingStore.kt) 
is the persistence layer for PBGs and their charges. There are example reference implementations that can be used out of the box. [[1]](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/InMemoryBackingStore.kt), [[2]](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/deploy/postgres/PostgresBackingStore.kt).
<br /> Finding which PBGs to charge for a given event filter leverages CEL expressions by passing in PBG fields as [operative fields](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration/EventFilters.kt#L36).
([example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/testing/TestPrivacyBucketMapper.kt#L39))
### Changing PBG definition
If you want to switch the definition of PBG, here are the steps to follow:

* Change the [privacy Bucket group class](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBucketGroup.kt#L31)
* Change the [privacy bucket filter class](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBucketFilter.kt#L72)
* Change the [implement a mapper with the desired fields](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBucketMapper.kt#L20)
    * Here is an [example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/testing/TestPrivacyBucketMapper.kt#L33)
* Change the [backing store implementation](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/55f6ea378f7386df5e30f072890c4fc5c2b73a4d/src/main/kotlin/org/wfanet/measurement/integration/deploy/postgres/ledger.sql#L25)

### Changing accounting levels
If you want to switch to a more granular level accounting such as event group level, here are the steps to follow:

* Change the measurementConsumerId field to the desired id (eventGroupId) in the
[reference class](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/55f6ea378f7386df5e30f072890c4fc5c2b73a4d/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyQuery.kt#L25)
* Change the measurementConsumerId field to the desired id (eventGroupId)  in the [filter class](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBucketFilter.kt#L32)
* Change the measurementConsumerId field to the desired id (eventGroupId)  in the [privacy Bucket group class](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/PrivacyBucketGroup.kt#L31)
* Change the measurementConsumerId field to the desired id (eventGroupId)  in your [backing store implementation](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/55f6ea378f7386df5e30f072890c4fc5c2b73a4d/src/main/kotlin/org/wfanet/measurement/integration/deploy/postgres/ledger.sql#L25)

