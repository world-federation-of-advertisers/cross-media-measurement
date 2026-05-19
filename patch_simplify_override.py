import sys

# Patch InProcessEdpAggregatorComponents.kt
filepath1 = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/InProcessEdpAggregatorComponents.kt"
with open(filepath1, "r") as f:
    content1 = f.read()

# 1. Simplify EventGroupEntityOverride
old_override = '''data class EventGroupEntityOverride(
  val entityKey: EventGroup.EntityKey? = null,
  val entityMetadata: Struct? = null,
  val additionalBlobEntityKeys: List<EntityKey> = emptyList(),
)'''
new_override = '''data class EventGroupEntityOverride(
  val blobEntityKeys: List<EntityKey> = emptyList(),
  val entityMetadata: Struct? = null,
)'''
content1 = content1.replace(old_override, new_override, 1)

# 2. Update event group creation to derive entityKey from blobEntityKeys.first()
old_entity_key_set = '''          if (override?.entityKey != null) {
            this.entityKey = override.entityKey
          }'''
new_entity_key_set = '''          if (override != null && override.blobEntityKeys.isNotEmpty()) {
            this.entityKey = edpaEntityKey {
              entityType = override.blobEntityKeys.first().entityType
              entityId = override.blobEntityKeys.first().entityId
            }
          }'''
content1 = content1.replace(old_entity_key_set, new_entity_key_set, 1)

# 3. Simplify impression data generation — use blobEntityKeys directly
old_gen = '''      val override: EventGroupEntityOverride? =
        entityOverridesByEdp[edpAggregatorShortName]?.get(mappedEventGroup.eventGroupReferenceId)
      val blobEntityKeys: List<EntityKey> =
        if (override != null) {
          listOf(
            EntityKey(
              entityType =
                requireNotNull(override.entityKey?.entityType) {
                  "Override for ${mappedEventGroup.eventGroupReferenceId} has no entityType"
                },
              entityId =
                requireNotNull(override.entityKey?.entityId) {
                  "Override for ${mappedEventGroup.eventGroupReferenceId} has no entityId"
                },
            )
          )
        } else {
          emptyList()
        }
      val entityKeyedEvents: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
        if (override != null && override.additionalBlobEntityKeys.isNotEmpty()) {
          events.map { shard ->
            val materializedEvents = shard.labeledEvents.toList()
            val splitPoint = materializedEvents.size / 2
            EntityKeyedLabeledEventDateShard(
              shard.localDate,
              sequenceOf(
                EntityKeysWithLabeledEvents(
                  blobEntityKeys,
                  materializedEvents.take(splitPoint).asSequence(),
                ),
                EntityKeysWithLabeledEvents(
                  override.additionalBlobEntityKeys,
                  materializedEvents.drop(splitPoint).asSequence(),
                ),
              ),
            )
          }
        } else {
          events.map {
            EntityKeyedLabeledEventDateShard(
              it.localDate,
              sequenceOf(EntityKeysWithLabeledEvents(blobEntityKeys, it.labeledEvents)),
            )
          }
        }'''

new_gen = '''      val override: EventGroupEntityOverride? =
        entityOverridesByEdp[edpAggregatorShortName]?.get(mappedEventGroup.eventGroupReferenceId)
      val blobEntityKeys: List<EntityKey> = override?.blobEntityKeys ?: emptyList()
      val entityKeyedEvents: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
        if (blobEntityKeys.size > 1) {
          events.map { shard ->
            val materializedEvents = shard.labeledEvents.toList()
            val chunkSize = materializedEvents.size / blobEntityKeys.size
            EntityKeyedLabeledEventDateShard(
              shard.localDate,
              blobEntityKeys.mapIndexed { i, entityKey ->
                val start = i * chunkSize
                val end = if (i == blobEntityKeys.lastIndex) materializedEvents.size else start + chunkSize
                EntityKeysWithLabeledEvents(
                  listOf(entityKey),
                  materializedEvents.subList(start, end).asSequence(),
                )
              }.asSequence(),
            )
          }
        } else {
          events.map {
            EntityKeyedLabeledEventDateShard(
              it.localDate,
              sequenceOf(EntityKeysWithLabeledEvents(blobEntityKeys, it.labeledEvents)),
            )
          }
        }'''

content1 = content1.replace(old_gen, new_gen, 1)

# 4. Alias the entityKey import so it doesn't conflict with DSL scope
old_import = 'import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey\n'
new_import = 'import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as edpaEntityKey\n'
content1 = content1.replace(old_import, new_import, 1)

with open(filepath1, "w") as f:
    f.write(content1)
print("OK: InProcessEdpAggregatorComponents simplified")


# Patch InProcessEdpAggregatorLifeOfAReportTest.kt
filepath2 = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessEdpAggregatorLifeOfAReportTest.kt"
with open(filepath2, "r") as f:
    content2 = f.read()

# 5. Update creativeIdOverride helper
old_helper = '''    fun creativeIdOverride(refId: String) =
      EventGroupEntityOverride(
        entityKey =
          edpaEntityKey {
            entityType = CREATIVE_ID_ENTITY_TYPE
            entityId = refId
          },
        entityMetadata = ENTITY_METADATA,
      )'''
new_helper = '''    fun creativeIdOverride(refId: String) =
      EventGroupEntityOverride(
        blobEntityKeys = listOf(EntityKey(CREATIVE_ID_ENTITY_TYPE, refId)),
        entityMetadata = ENTITY_METADATA,
      )'''
content2 = content2.replace(old_helper, new_helper, 1)

# 6. Update campaignOverride helper
old_campaign = '''    fun campaignOverride(refId: String) =
      EventGroupEntityOverride(
        entityKey =
          edpaEntityKey {
            entityType = "campaign"
            entityId = refId
          },
        entityMetadata = ENTITY_METADATA,
      )'''
new_campaign = '''    fun campaignOverride(refId: String) =
      EventGroupEntityOverride(
        blobEntityKeys = listOf(EntityKey("campaign", refId)),
        entityMetadata = ENTITY_METADATA,
      )'''
content2 = content2.replace(old_campaign, new_campaign, 1)

# 7. Update multi-entity-key override
old_multi = '''        EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            entityKey =
              edpaEntityKey {
                entityType = CREATIVE_ID_ENTITY_TYPE
                entityId = EDP1_MULTI_CREATIVE_A_ID
              },
            entityMetadata = ENTITY_METADATA,
            additionalBlobEntityKeys =
              listOf(
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID)
              ),
          ),'''
new_multi = '''        EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            blobEntityKeys =
              listOf(
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_A_ID),
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID),
              ),
            entityMetadata = ENTITY_METADATA,
          ),'''
content2 = content2.replace(old_multi, new_multi, 1)

# 8. Update edp4 ad_group override
old_adgroup = '''        AD_GROUP_EDP_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            entityKey =
              edpaEntityKey {
                entityType = "ad_group"
                entityId = AD_GROUP_EDP_EVENT_GROUP_REF_ID
              },
            entityMetadata = ENTITY_METADATA,
          )'''
new_adgroup = '''        AD_GROUP_EDP_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            blobEntityKeys = listOf(EntityKey("ad_group", AD_GROUP_EDP_EVENT_GROUP_REF_ID)),
            entityMetadata = ENTITY_METADATA,
          )'''
content2 = content2.replace(old_adgroup, new_adgroup, 1)

# 9. Remove unused edpaEntityKey import from test file if it exists
# (The builder is now only used in InProcessEdpAggregatorComponents)
old_edpa_import = '''import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as edpaEntityKey
'''
if old_edpa_import in content2:
    content2 = content2.replace(old_edpa_import, '', 1)

with open(filepath2, "w") as f:
    f.write(content2)
print("OK: InProcessEdpAggregatorLifeOfAReportTest simplified")
