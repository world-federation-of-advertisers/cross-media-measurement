import sys

# Patch 1: AbstractEdpAggregatorCorrectnessTest - add constant and test
filepath1 = "/home/stevenwjones/cross-media-measurement/src/test/kotlin/org/wfanet/measurement/integration/k8s/AbstractEdpAggregatorCorrectnessTest.kt"
with open(filepath1, "r") as f:
    content1 = f.read()

# 1a. Add MULTI_CREATIVE constant
old_constants = '''    const val CREATIVE_ID_EVENT_GROUP_REF_ID = "edpa-eg-creative-id-1"'''
new_constants = '''    const val CREATIVE_ID_EVENT_GROUP_REF_ID = "edpa-eg-creative-id-1"
    const val MULTI_CREATIVE_EVENT_GROUP_REF_ID = "edpa-eg-multi-creative-1"'''
content1 = content1.replace(old_constants, new_constants, 1)

# 1b. Add multi-entity-key test after creative-id test
old_creative_test = '''  @Test
  fun `direct measurement with creative-id entity-key-only event groups succeeds`() = runBlocking {
    mcSimulator.testDirectReachAndFrequency(
      "1241",
      1,
      eventGroupFilter = { it.eventGroupReferenceId == CREATIVE_ID_EVENT_GROUP_REF_ID },
    )
  }

  interface MeasurementSystem {'''

new_creative_test = '''  @Test
  fun `direct measurement with creative-id entity-key-only event groups succeeds`() = runBlocking {
    mcSimulator.testDirectReachAndFrequency(
      "1241",
      1,
      eventGroupFilter = { it.eventGroupReferenceId == CREATIVE_ID_EVENT_GROUP_REF_ID },
    )
  }

  @Test
  fun `direct measurement with multi-entity-key blob filtering to one entity key succeeds`() =
    runBlocking {
      mcSimulator.testDirectReachAndFrequency(
        "1242",
        1,
        eventGroupFilter = { it.eventGroupReferenceId == MULTI_CREATIVE_EVENT_GROUP_REF_ID },
      )
    }

  interface MeasurementSystem {'''

content1 = content1.replace(old_creative_test, new_creative_test, 1)

with open(filepath1, "w") as f:
    f.write(content1)
print("OK: AbstractEdpAggregatorCorrectnessTest updated")


# Patch 2: EdpAggregatorCorrectnessTest - add event group setup
filepath2 = "/home/stevenwjones/cross-media-measurement/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt"
with open(filepath2, "r") as f:
    content2 = f.read()

# 2a. Add multi-creative to edp7's eventGroupReferenceIds
old_edp7_storage = '''          eventGroupReferenceIds =
            setOf(EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID, CREATIVE_ID_EVENT_GROUP_REF_ID),'''
new_edp7_storage = '''          eventGroupReferenceIds =
            setOf(
              EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID,
              CREATIVE_ID_EVENT_GROUP_REF_ID,
              MULTI_CREATIVE_EVENT_GROUP_REF_ID,
            ),'''
content2 = content2.replace(old_edp7_storage, new_edp7_storage, 1)

# 2b. Update createEventGroups to handle multi-creative entity key
old_create = '''          val hasCreativeIdEntityKey = eventGroupReferenceId == CREATIVE_ID_EVENT_GROUP_REF_ID
          eventGroup {
            this.eventGroupReferenceId = eventGroupReferenceId
            measurementConsumer = TEST_CONFIG.measurementConsumer
            dataAvailabilityInterval = interval {
              this.startTime = timestamp { seconds = startTime.epochSecond }
              this.endTime = timestamp { seconds = endTime.epochSecond }
            }
            this.eventGroupMetadata = eventGroupMetadata {
              this.adMetadata = adMetadata {
                this.campaignMetadata = campaignMetadata {
                  brand = "some-brand"
                  campaign = "some-campaign"
                }
              }
              if (hasCreativeIdEntityKey) {
                this.entityMetadata = struct {
                  fields["placement"] = value { stringValue = "homepage_top" }
                  fields["objective"] = value { stringValue = "awareness" }
                }
              }
            }
            if (hasCreativeIdEntityKey) {
              this.entityKey = entityKey {
                entityType = "creative-id"
                entityId = eventGroupReferenceId
              }
            }
            mediaTypes += MediaType.valueOf("VIDEO")
          }'''

new_create = '''          val hasCreativeIdEntityKey =
            eventGroupReferenceId in
              setOf(CREATIVE_ID_EVENT_GROUP_REF_ID, MULTI_CREATIVE_EVENT_GROUP_REF_ID)
          eventGroup {
            this.eventGroupReferenceId = eventGroupReferenceId
            measurementConsumer = TEST_CONFIG.measurementConsumer
            dataAvailabilityInterval = interval {
              this.startTime = timestamp { seconds = startTime.epochSecond }
              this.endTime = timestamp { seconds = endTime.epochSecond }
            }
            this.eventGroupMetadata = eventGroupMetadata {
              this.adMetadata = adMetadata {
                this.campaignMetadata = campaignMetadata {
                  brand = "some-brand"
                  campaign = "some-campaign"
                }
              }
              if (hasCreativeIdEntityKey) {
                this.entityMetadata = struct {
                  fields["placement"] = value { stringValue = "homepage_top" }
                  fields["objective"] = value { stringValue = "awareness" }
                }
              }
            }
            if (hasCreativeIdEntityKey) {
              this.entityKey = entityKey {
                entityType = "creative-id"
                entityId = eventGroupReferenceId
              }
            }
            mediaTypes += MediaType.valueOf("VIDEO")
          }'''

content2 = content2.replace(old_create, new_create, 1)

# 2c. Add multi-creative to syntheticEventGroupMap
old_map = '''    val syntheticEventGroupMap =
      mapOf(
        EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
        CREATIVE_ID_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
        EDPA_META_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
      )'''

new_map = '''    val syntheticEventGroupMap =
      mapOf(
        EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
        CREATIVE_ID_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
        MULTI_CREATIVE_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
        EDPA_META_EVENT_GROUP_REF_ID to syntheticEventGroupSpec,
      )'''

content2 = content2.replace(old_map, new_map, 1)

with open(filepath2, "w") as f:
    f.write(content2)
print("OK: EdpAggregatorCorrectnessTest updated")
