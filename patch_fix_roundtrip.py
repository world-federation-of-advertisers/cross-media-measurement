filepath = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessEdpAggregatorLifeOfAReportTest.kt"
with open(filepath, "r") as f:
    content = f.read()

old_assert = '''        assertWithMessage("entity_key.entity_type for $refId")
          .that(eventGroup.entityKey.entityType)
          .isEqualTo(override.entityKey!!.entityType)
        assertWithMessage("entity_key.entity_id for $refId")
          .that(eventGroup.entityKey.entityId)
          .isEqualTo(override.entityKey!!.entityId)'''

new_assert = '''        val expectedEntityKey = override.blobEntityKeys.first()
        assertWithMessage("entity_key.entity_type for $refId")
          .that(eventGroup.entityKey.entityType)
          .isEqualTo(expectedEntityKey.entityType)
        assertWithMessage("entity_key.entity_id for $refId")
          .that(eventGroup.entityKey.entityId)
          .isEqualTo(expectedEntityKey.entityId)'''

content = content.replace(old_assert, new_assert, 1)

with open(filepath, "w") as f:
    f.write(content)
print("OK: fixed entity key round-trip assertion")
