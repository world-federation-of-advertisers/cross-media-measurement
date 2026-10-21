// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.eventgroups

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.timestamp
import com.google.type.interval
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.copy
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup

@RunWith(JUnit4::class)
class EventGroupValidatorTest {
  @Test
  fun `validate accepts valid EventGroup`() {
    EventGroupValidator.validate(VALID_EVENT_GROUP)
  }

  @Test
  fun `validate accepts EventGroup with client account reference ID`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        measurementConsumer = ""
        clientAccountReferenceId = "account-1"
      }

    EventGroupValidator.validate(eventGroup)
  }

  @Test
  fun `validate accepts EventGroup with legacy reference ID only`() {
    val eventGroup = VALID_EVENT_GROUP.copy { clearEntityKey() }

    EventGroupValidator.validate(eventGroup)
  }

  @Test
  fun `validate accepts EventGroup with entity key only`() {
    val eventGroup = VALID_EVENT_GROUP.copy { eventGroupReferenceId = "" }

    EventGroupValidator.validate(eventGroup)
  }

  @Test
  fun `validate accepts EventGroup with open-ended interval`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval { startTime = timestamp { seconds = 200 } }
      }

    EventGroupValidator.validate(eventGroup)
  }

  @Test
  fun `validate accepts deleted EventGroup`() {
    val eventGroup = eventGroup {
      state = EventGroup.State.DELETED
      measurementConsumer = "measurementConsumers/mc-1"
      entityKey = entityKey {
        entityType = "campaign"
        entityId = "campaign-1"
      }
    }

    EventGroupValidator.validate(eventGroup)
  }

  @Test
  fun `validate rejects equal interval endpoints`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 200 }
        }
      }

    assertInvalid(eventGroup, "start_time must be before end_time")
  }

  @Test
  fun `validate rejects reversed interval endpoints`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 300 }
          endTime = timestamp { seconds = 200 }
        }
      }

    assertInvalid(eventGroup, "start_time must be before end_time")
  }

  @Test
  fun `validate rejects invalid start timestamp`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp {
            seconds = 200
            nanos = 1_000_000_000
          }
          endTime = timestamp { seconds = 300 }
        }
      }

    assertInvalid(eventGroup, "start_time must be a valid timestamp")
  }

  @Test
  fun `validate rejects invalid end timestamp`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp {
            seconds = 300
            nanos = 1_000_000_000
          }
        }
      }

    assertInvalid(eventGroup, "end_time must be a valid timestamp")
  }

  @Test
  fun `validate rejects missing start time`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval { endTime = timestamp { seconds = 300 } }
      }

    assertInvalid(eventGroup, "start_time must be a valid timestamp")
  }

  @Test
  fun `validate rejects zero start time`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp {}
          endTime = timestamp { seconds = 300 }
        }
      }

    assertInvalid(eventGroup, "start_time required")
  }

  @Test
  fun `validate rejects missing data availability interval`() {
    val eventGroup = VALID_EVENT_GROUP.copy { clearDataAvailabilityInterval() }

    assertInvalid(eventGroup, "Data availability must be set")
  }

  @Test
  fun `validate rejects bare measurement consumer ID`() {
    val eventGroup = VALID_EVENT_GROUP.copy { measurementConsumer = "mc-1" }

    assertInvalid(eventGroup, "expected measurementConsumers/<measurement-consumer-id>")
  }

  @Test
  fun `validate rejects measurement consumer resource name with empty ID`() {
    val eventGroup = VALID_EVENT_GROUP.copy { measurementConsumer = "measurementConsumers/" }

    assertInvalid(eventGroup, "Malformed measurement_consumer")
  }

  @Test
  fun `validate rejects missing advertiser identifiers`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        measurementConsumer = ""
        clientAccountReferenceId = ""
      }

    assertInvalid(eventGroup, "Either Measurement Consumer or Client Account Reference ID")
  }

  @Test
  fun `validate rejects client account reference ID longer than 36 characters`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        measurementConsumer = ""
        clientAccountReferenceId = "a".repeat(37)
      }

    assertInvalid(eventGroup, "client_account_reference_id must not exceed 36 characters")
  }

  @Test
  fun `validate rejects long client account reference ID when measurement consumer is set`() {
    val eventGroup = VALID_EVENT_GROUP.copy { clientAccountReferenceId = "a".repeat(37) }

    assertInvalid(eventGroup, "client_account_reference_id must not exceed 36 characters")
  }

  @Test
  fun `validate rejects missing media type`() {
    val eventGroup = VALID_EVENT_GROUP.copy { mediaTypes.clear() }

    assertInvalid(eventGroup, "At least one media type")
  }

  @Test
  fun `validate rejects unspecified media type`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy { mediaTypes += EventGroup.MediaType.MEDIA_TYPE_UNSPECIFIED }

    assertInvalid(eventGroup, "must not contain MEDIA_TYPE_UNSPECIFIED")
  }

  @Test
  fun `validate rejects unrecognized media type`() {
    val eventGroup = withUnknownEnumValue(VALID_EVENT_GROUP, EventGroup.MEDIA_TYPES_FIELD_NUMBER)

    assertInvalid(eventGroup, "media_types contains an unrecognized value")
  }

  @Test
  fun `validate rejects unrecognized state`() {
    val eventGroup = withUnknownEnumValue(VALID_EVENT_GROUP, EventGroup.STATE_FIELD_NUMBER)

    assertInvalid(eventGroup, "state must be STATE_UNSPECIFIED, ACTIVE, or DELETED")
  }

  @Test
  fun `validate rejects missing EventGroup metadata`() {
    val eventGroup = VALID_EVENT_GROUP.copy { clearEventGroupMetadata() }

    assertInvalid(eventGroup, "Event Group Metadata must be set")
  }

  @Test
  fun `validate rejects missing ad metadata`() {
    val eventGroup = VALID_EVENT_GROUP.copy { eventGroupMetadata = metadata {} }

    assertInvalid(eventGroup, "ad_metadata must be set")
  }

  @Test
  fun `validate rejects missing campaign metadata`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy { eventGroupMetadata = metadata { adMetadata = adMetadata {} } }

    assertInvalid(eventGroup, "campaign_metadata must be set")
  }

  @Test
  fun `validate rejects empty brand`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        eventGroupMetadata = metadata {
          adMetadata = adMetadata {
            campaignMetadata = campaignMetadata { campaign = "Example campaign" }
          }
        }
      }

    assertInvalid(eventGroup, "brand must be non-blank")
  }

  @Test
  fun `validate rejects blank brand`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        eventGroupMetadata = metadata {
          adMetadata = adMetadata {
            campaignMetadata = campaignMetadata {
              brand = " "
              campaign = "Example campaign"
            }
          }
        }
      }

    assertInvalid(eventGroup, "brand must be non-blank")
  }

  @Test
  fun `validate rejects empty campaign`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        eventGroupMetadata = metadata {
          adMetadata = adMetadata {
            campaignMetadata = campaignMetadata { brand = "Example brand" }
          }
        }
      }

    assertInvalid(eventGroup, "campaign must be non-blank")
  }

  @Test
  fun `validate rejects blank campaign`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        eventGroupMetadata = metadata {
          adMetadata = adMetadata {
            campaignMetadata = campaignMetadata {
              brand = "Example brand"
              campaign = " "
            }
          }
        }
      }

    assertInvalid(eventGroup, "campaign must be non-blank")
  }

  @Test
  fun `validate rejects missing EventGroup identifiers`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        eventGroupReferenceId = ""
        clearEntityKey()
      }

    assertInvalid(eventGroup, "Either Event Group Reference Id or Entity Key")
  }

  @Test
  fun `validate rejects blank legacy reference ID without entity key`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        eventGroupReferenceId = " "
        clearEntityKey()
      }

    assertInvalid(eventGroup, "Either Event Group Reference Id or Entity Key")
  }

  @Test
  fun `validate rejects entity key with missing type`() {
    val eventGroup = VALID_EVENT_GROUP.copy { entityKey = entityKey { entityId = "campaign-1" } }

    assertInvalid(eventGroup, "entity_type must be non-blank")
  }

  @Test
  fun `validate rejects entity key with missing ID`() {
    val eventGroup = VALID_EVENT_GROUP.copy { entityKey = entityKey { entityType = "campaign" } }

    assertInvalid(eventGroup, "entity_id must be non-blank")
  }

  @Test
  fun `validate rejects URL-unsafe entity type`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        entityKey = entityKey {
          entityType = "campaign/type"
          entityId = "campaign-1"
        }
      }

    assertInvalid(eventGroup, "entity_type must be URL-safe")
  }

  @Test
  fun `validate rejects URL-unsafe entity ID`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        entityKey = entityKey {
          entityType = "campaign"
          entityId = "campaign/1"
        }
      }

    assertInvalid(eventGroup, "entity_id must be URL-safe")
  }

  @Test
  fun `validate rejects deleted EventGroup without entity key`() {
    val eventGroup = eventGroup {
      state = EventGroup.State.DELETED
      measurementConsumer = "measurementConsumers/mc-1"
    }

    assertInvalid(eventGroup, "Entity Key with entity ID")
  }

  @Test
  fun `validate rejects deleted EventGroup without measurement consumer`() {
    val eventGroup = eventGroup {
      state = EventGroup.State.DELETED
      entityKey = entityKey {
        entityType = "campaign"
        entityId = "campaign-1"
      }
    }

    assertInvalid(eventGroup, "Measurement Consumer must be set")
  }

  @Test
  fun `validate rejects deleted EventGroup with only client account reference ID`() {
    val eventGroup = eventGroup {
      state = EventGroup.State.DELETED
      clientAccountReferenceId = "account-1"
      entityKey = entityKey {
        entityType = "campaign"
        entityId = "campaign-1"
      }
    }

    assertInvalid(eventGroup, "Measurement Consumer must be set")
  }

  @Test
  fun `validate rejects malformed measurement consumer on deleted EventGroup`() {
    val eventGroup = eventGroup {
      state = EventGroup.State.DELETED
      measurementConsumer = "mc-1"
      entityKey = entityKey {
        entityType = "campaign"
        entityId = "campaign-1"
      }
    }

    assertInvalid(eventGroup, "Malformed measurement_consumer")
  }

  private fun assertInvalid(eventGroup: EventGroup, messageSubstring: String) {
    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }
    assertThat(exception).hasMessageThat().contains(messageSubstring)
  }

  private fun withUnknownEnumValue(eventGroup: EventGroup, fieldNumber: Int): EventGroup {
    val output = ByteString.newOutput()
    val codedOutput = CodedOutputStream.newInstance(output)
    eventGroup.writeTo(codedOutput)
    codedOutput.writeEnum(fieldNumber, UNKNOWN_ENUM_VALUE)
    codedOutput.flush()
    return EventGroup.parseFrom(output.toByteString())
  }

  private companion object {
    const val UNKNOWN_ENUM_VALUE = 999

    val VALID_EVENT_GROUP = eventGroup {
      eventGroupReferenceId = "campaign-1"
      eventGroupMetadata = metadata {
        adMetadata = adMetadata {
          campaignMetadata = campaignMetadata {
            brand = "Example brand"
            campaign = "Example campaign"
          }
        }
      }
      dataAvailabilityInterval = interval {
        startTime = timestamp { seconds = 200 }
        endTime = timestamp { seconds = 300 }
      }
      measurementConsumer = "measurementConsumers/mc-1"
      mediaTypes += EventGroup.MediaType.VIDEO
      entityKey = entityKey {
        entityType = "campaign"
        entityId = "campaign-1"
      }
    }
  }
}
