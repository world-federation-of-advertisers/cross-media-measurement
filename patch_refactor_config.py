import sys

# ==============================
# Patch InProcessEdpAggregatorComponents.kt
# ==============================
filepath1 = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/InProcessEdpAggregatorComponents.kt"
with open(filepath1, "r") as f:
    content1 = f.read()

# 1. Replace EventGroupEntityOverride with EventGroupConfig
old_override = '''data class EventGroupEntityOverride(
  val blobEntityKeys: List<EntityKey> = emptyList(),
  val entityMetadata: Struct? = null,
)'''
new_override = '''data class EventGroupConfig(
  val spec: SyntheticEventGroupSpec,
  val blobEntityKeys: List<EntityKey> = emptyList(),
  val entityMetadata: Struct? = null,
)'''
content1 = content1.replace(old_override, new_override, 1)

# 2. Replace the two constructor params with one
old_params = '''  private val syntheticEventGroupMapByEdp: Map<String, Map<String, SyntheticEventGroupSpec>>,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val externalKmsClient: FakeKmsClient,
  private val entityOverridesByEdp: Map<String, Map<String, EventGroupEntityOverride>> = emptyMap(),'''
new_params = '''  private val eventGroupConfigsByEdp: Map<String, Map<String, EventGroupConfig>>,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val externalKmsClient: FakeKmsClient,'''
content1 = content1.replace(old_params, new_params, 1)

# 3. Update buildEventGroups — replace override lookup + syntheticEventGroupMapByEdp
old_build = '''  private fun buildEventGroups(
    measurementConsumerData: MeasurementConsumerData,
    edpAggregatorShortName: String,
  ): List<EventGroup> {
    val edpOverrides: Map<String, EventGroupEntityOverride> =
      entityOverridesByEdp[edpAggregatorShortName].orEmpty()
    return syntheticEventGroupMapByEdp.getValue(edpAggregatorShortName).flatMap {
      (eventGroupReferenceId, syntheticEventGroupSpec) ->
      val override: EventGroupEntityOverride? = edpOverrides[eventGroupReferenceId]
      syntheticEventGroupSpec.dateSpecsList.map { dateSpec ->'''
new_build = '''  private fun buildEventGroups(
    measurementConsumerData: MeasurementConsumerData,
    edpAggregatorShortName: String,
  ): List<EventGroup> {
    return eventGroupConfigsByEdp.getValue(edpAggregatorShortName).flatMap {
      (eventGroupReferenceId, config) ->
      config.spec.dateSpecsList.map { dateSpec ->'''
content1 = content1.replace(old_build, new_build, 1)

# 4. Update entityMetadata and entityKey setting in buildEventGroups
old_metadata = '''            if (override?.entityMetadata != null) {
              this.entityMetadata = override.entityMetadata
            }
          }
          if (override != null && override.blobEntityKeys.isNotEmpty()) {
            this.entityKey = edpaEntityKey {
              entityType = override.blobEntityKeys.first().entityType
              entityId = override.blobEntityKeys.first().entityId
            }
          }'''
new_metadata = '''            if (config.entityMetadata != null) {
              this.entityMetadata = config.entityMetadata
            }
          }
          if (config.blobEntityKeys.isNotEmpty()) {
            this.entityKey = edpaEntityKey {
              entityType = config.blobEntityKeys.first().entityType
              entityId = config.blobEntityKeys.first().entityId
            }
          }'''
content1 = content1.replace(old_metadata, new_metadata, 1)

# 5. Update writeImpressionData — replace both map lookups with single config lookup
old_write = '''    mappedEventGroups.forEach { mappedEventGroup ->
      val events: Sequence<LabeledEventDateShard<TestEvent>> =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          syntheticPopulationSpec,
          syntheticEventGroupMapByEdp
            .getValue(edpAggregatorShortName)
            .getValue(mappedEventGroup.eventGroupReferenceId),
        )'''
new_write = '''    mappedEventGroups.forEach { mappedEventGroup ->
      val config: EventGroupConfig =
        eventGroupConfigsByEdp
          .getValue(edpAggregatorShortName)
          .getValue(mappedEventGroup.eventGroupReferenceId)
      val events: Sequence<LabeledEventDateShard<TestEvent>> =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          syntheticPopulationSpec,
          config.spec,
        )'''
content1 = content1.replace(old_write, new_write, 1)

# 6. Replace override lookup in writeImpressionData with config
old_override_lookup = '''      val override: EventGroupEntityOverride? =
        entityOverridesByEdp[edpAggregatorShortName]?.get(mappedEventGroup.eventGroupReferenceId)
      val blobEntityKeys: List<EntityKey> = override?.blobEntityKeys ?: emptyList()'''
new_override_lookup = '''      val blobEntityKeys: List<EntityKey> = config.blobEntityKeys'''
content1 = content1.replace(old_override_lookup, new_override_lookup, 1)

# 7. Update the third usage site (impression metadata generation) that references syntheticEventGroupMapByEdp
# Find and replace the events generation for impression metadata
old_meta_events = '''        val events =
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            syntheticPopulationSpec,
            syntheticEventGroupMapByEdp
              .getValue(edpAggregatorShortName)
              .getValue(mappedEventGroup.eventGroupReferenceId),
          )'''
new_meta_events = '''        val metaConfig: EventGroupConfig =
          eventGroupConfigsByEdp
            .getValue(edpAggregatorShortName)
            .getValue(mappedEventGroup.eventGroupReferenceId)
        val events =
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            syntheticPopulationSpec,
            metaConfig.spec,
          )'''
content1 = content1.replace(old_meta_events, new_meta_events, 1)

with open(filepath1, "w") as f:
    f.write(content1)
print("OK: InProcessEdpAggregatorComponents refactored")


# ==============================
# Patch InProcessEdpAggregatorLifeOfAReportTest.kt
# ==============================
filepath2 = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessEdpAggregatorLifeOfAReportTest.kt"
with open(filepath2, "r") as f:
    content2 = f.read()

# 8. Replace both maps with a single eventGroupConfigsByEdp
old_both_maps = '''  private val syntheticEventGroupMapByEdp =
    mapOf(
      "edp1" to
        mapOf(
          "edp1-eg-ref-1" to syntheticEventGroupSpec2,
          "edp1-eg-creative-1" to syntheticEventGroupSpec2,
          EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to syntheticEventGroupSpec2,
        ),
      "edp2" to
        mapOf(
          "edp2-eg-ref-1" to syntheticEventGroupSpec1,
          "edp2-eg-creative-1" to syntheticEventGroupSpec1,
        ),
      "edp3" to mapOf("edp3-eg-ref-1" to syntheticEventGroupSpec2),
      "edp4" to mapOf("edp4-eg-ref-1" to syntheticEventGroupSpec1),
    )

  // Mix of entity_key shapes so the test exercises every path the workstream cares about:
  //   - edp1-eg-ref-1: no override (legacy path; Kingdom defaults entity_type="campaign",
  //     entity_id NULL).
  //   - edp1-eg-creative-1: overridden with entity_type="creative-id".
  //   - edp2-eg-ref-1, edp3-eg-ref-1: overridden with entity_type="campaign" + entity_id.
  //   - edp2-eg-creative-1: overridden with entity_type="creative-id".
  //   - edp4-eg-ref-1: overridden with entity_type="ad_group" (non-default type round-trip).
  // listReportingEventGroups() filters entity_type_in=["campaign", "ad_group", "creative-id"]
  // so all event groups are visible.
  private val entityOverridesByEdp: Map<String, Map<String, EventGroupEntityOverride>> = buildMap {
    fun creativeIdOverride(refId: String) =
      EventGroupEntityOverride(
        blobEntityKeys = listOf(EntityKey(CREATIVE_ID_ENTITY_TYPE, refId)),
        entityMetadata = ENTITY_METADATA,
      )

    fun campaignOverride(refId: String) =
      EventGroupEntityOverride(
        blobEntityKeys = listOf(EntityKey("campaign", refId)),
        entityMetadata = ENTITY_METADATA,
      )

    // edp1: edp1-eg-ref-1 has NO override (legacy); edp1-eg-creative-1 has creative-id;
    //       edp1-eg-multi-creative has two creative-id entity keys in the same blob.
    put(
      EDP_NO_ENTITY_KEY_DISPLAY_NAME,
      mapOf(
        EDP1_CREATIVE_EVENT_GROUP_REF_ID to creativeIdOverride(EDP1_CREATIVE_EVENT_GROUP_REF_ID),
        EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            blobEntityKeys =
              listOf(
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_A_ID),
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID),
              ),
            entityMetadata = ENTITY_METADATA,
          ),
      ),
    )
    // edp2: edp2-eg-ref-1 has campaign; edp2-eg-creative-1 has creative-id.
    put(
      "edp2",
      mapOf(
        "edp2-eg-ref-1" to campaignOverride("edp2-eg-ref-1"),
        EDP2_CREATIVE_EVENT_GROUP_REF_ID to creativeIdOverride(EDP2_CREATIVE_EVENT_GROUP_REF_ID),
      ),
    )
    // edp3: campaign.
    put("edp3", mapOf("edp3-eg-ref-1" to campaignOverride("edp3-eg-ref-1")))
    // edp4: ad_group.
    put(
      AD_GROUP_EDP_DISPLAY_NAME,
      mapOf(
        AD_GROUP_EDP_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            blobEntityKeys = listOf(EntityKey("ad_group", AD_GROUP_EDP_EVENT_GROUP_REF_ID)),
            entityMetadata = ENTITY_METADATA,
          )
      ),
    )
  }'''

new_merged_map = '''  // Each event group config combines the synthetic data spec with entity key configuration.
  //   - edp1-eg-ref-1: no entity key (legacy; Kingdom defaults entity_type="campaign").
  //   - edp1-eg-creative-1: entity_type="creative-id".
  //   - edp1-eg-multi-creative: two creative-id entity keys in the same blob.
  //   - edp2-eg-ref-1, edp3-eg-ref-1: entity_type="campaign" + entity_id.
  //   - edp2-eg-creative-1: entity_type="creative-id".
  //   - edp4-eg-ref-1: entity_type="ad_group" (non-default type round-trip).
  // listReportingEventGroups() filters entity_type_in=["campaign", "ad_group", "creative-id"]
  // so all event groups are visible.
  private val eventGroupConfigsByEdp: Map<String, Map<String, EventGroupConfig>> =
    mapOf(
      EDP_NO_ENTITY_KEY_DISPLAY_NAME to
        mapOf(
          "edp1-eg-ref-1" to EventGroupConfig(syntheticEventGroupSpec2),
          EDP1_CREATIVE_EVENT_GROUP_REF_ID to
            EventGroupConfig(
              syntheticEventGroupSpec2,
              blobEntityKeys = listOf(EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_CREATIVE_EVENT_GROUP_REF_ID)),
              entityMetadata = ENTITY_METADATA,
            ),
          EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to
            EventGroupConfig(
              syntheticEventGroupSpec2,
              blobEntityKeys =
                listOf(
                  EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_A_ID),
                  EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID),
                ),
              entityMetadata = ENTITY_METADATA,
            ),
        ),
      "edp2" to
        mapOf(
          "edp2-eg-ref-1" to
            EventGroupConfig(
              syntheticEventGroupSpec1,
              blobEntityKeys = listOf(EntityKey("campaign", "edp2-eg-ref-1")),
              entityMetadata = ENTITY_METADATA,
            ),
          EDP2_CREATIVE_EVENT_GROUP_REF_ID to
            EventGroupConfig(
              syntheticEventGroupSpec1,
              blobEntityKeys = listOf(EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP2_CREATIVE_EVENT_GROUP_REF_ID)),
              entityMetadata = ENTITY_METADATA,
            ),
        ),
      "edp3" to
        mapOf(
          "edp3-eg-ref-1" to
            EventGroupConfig(
              syntheticEventGroupSpec2,
              blobEntityKeys = listOf(EntityKey("campaign", "edp3-eg-ref-1")),
              entityMetadata = ENTITY_METADATA,
            ),
        ),
      AD_GROUP_EDP_DISPLAY_NAME to
        mapOf(
          AD_GROUP_EDP_EVENT_GROUP_REF_ID to
            EventGroupConfig(
              syntheticEventGroupSpec1,
              blobEntityKeys = listOf(EntityKey("ad_group", AD_GROUP_EDP_EVENT_GROUP_REF_ID)),
              entityMetadata = ENTITY_METADATA,
            ),
        ),
    )'''

content2 = content2.replace(old_both_maps, new_merged_map, 1)

# 9. Update the constructor call
old_ctor = '''      syntheticEventGroupMapByEdp = syntheticEventGroupMapByEdp,
      syntheticPopulationSpec = syntheticPopulationSpec,
      modelLineInfoMap = modelLineInfoMap,
      externalKmsClient = sharedKmsClient,
      entityOverridesByEdp = entityOverridesByEdp,'''
new_ctor = '''      eventGroupConfigsByEdp = eventGroupConfigsByEdp,
      syntheticPopulationSpec = syntheticPopulationSpec,
      modelLineInfoMap = modelLineInfoMap,
      externalKmsClient = sharedKmsClient,'''
content2 = content2.replace(old_ctor, new_ctor, 1)

# 10. Update the entity key round-trip test
old_roundtrip = '''    for ((_, refOverrides) in entityOverridesByEdp) {
      for ((refId, override) in refOverrides) {
        val eventGroup = byRefId.getValue(refId)
        val expectedEntityKey = override.blobEntityKeys.first()
        assertWithMessage("entity_key.entity_type for $refId")
          .that(eventGroup.entityKey.entityType)
          .isEqualTo(expectedEntityKey.entityType)
        assertWithMessage("entity_key.entity_id for $refId")
          .that(eventGroup.entityKey.entityId)
          .isEqualTo(expectedEntityKey.entityId)
        assertWithMessage("entity_metadata for $refId")
          .that(eventGroup.eventGroupMetadata.entityMetadata)
          .isEqualTo(override.entityMetadata)
      }
    }'''
new_roundtrip = '''    for ((_, refConfigs) in eventGroupConfigsByEdp) {
      for ((refId, config) in refConfigs) {
        if (config.blobEntityKeys.isEmpty()) continue
        val eventGroup = byRefId.getValue(refId)
        val expectedEntityKey = config.blobEntityKeys.first()
        assertWithMessage("entity_key.entity_type for $refId")
          .that(eventGroup.entityKey.entityType)
          .isEqualTo(expectedEntityKey.entityType)
        assertWithMessage("entity_key.entity_id for $refId")
          .that(eventGroup.entityKey.entityId)
          .isEqualTo(expectedEntityKey.entityId)
        assertWithMessage("entity_metadata for $refId")
          .that(eventGroup.eventGroupMetadata.entityMetadata)
          .isEqualTo(config.entityMetadata)
      }
    }'''
content2 = content2.replace(old_roundtrip, new_roundtrip, 1)

# 11. Remove imports that are no longer needed
content2 = content2.replace('import org.wfanet.measurement.integration.common.EventGroupEntityOverride\n', '', 1)

# 12. Add import for EventGroupConfig
old_config_import = 'import org.wfanet.measurement.integration.common.InProcessEdpAggregatorComponents'
new_config_import = '''import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.InProcessEdpAggregatorComponents'''
content2 = content2.replace(old_config_import, new_config_import, 1)

with open(filepath2, "w") as f:
    f.write(content2)
print("OK: InProcessEdpAggregatorLifeOfAReportTest refactored")
