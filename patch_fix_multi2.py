filepath = "/home/stevenwjones/cross-media-measurement/src/main/kotlin/org/wfanet/measurement/integration/common/reporting/v2/InProcessEdpAggregatorLifeOfAReportTest.kt"
with open(filepath, "r") as f:
    content = f.read()

# Fix the multi-entity-key override that the previous patch missed
old = '''        EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            entityKey =
              edpaEntityKey {
                entityType = CREATIVE_ID_ENTITY_TYPE
                entityId = EDP1_MULTI_CREATIVE_A_ID
              },
            entityMetadata = ENTITY_METADATA,
            additionalBlobEntityKeys =
              listOf(EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID)),
          ),'''

new = '''        EDP1_MULTI_ENTITY_KEY_EVENT_GROUP_REF_ID to
          EventGroupEntityOverride(
            blobEntityKeys =
              listOf(
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_A_ID),
                EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID),
              ),
            entityMetadata = ENTITY_METADATA,
          ),'''

if old in content:
    content = content.replace(old, new, 1)
    print("OK: fixed multi-entity-key override")
else:
    print("ERROR: could not find multi-entity-key override text")
    # Debug: show what's actually there
    import re
    m = re.search(r'EDP1_MULTI_ENTITY_KEY.*?(?=\n    \))', content, re.DOTALL)
    if m:
        print(f"Found: {m.group()[:200]}")

with open(filepath, "w") as f:
    f.write(content)
