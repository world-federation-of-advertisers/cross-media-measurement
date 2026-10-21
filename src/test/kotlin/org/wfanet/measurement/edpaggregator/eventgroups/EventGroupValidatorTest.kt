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
  fun `validate rejects equal interval endpoints`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        dataAvailabilityInterval = interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 200 }
        }
      }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception)
      .hasMessageThat()
      .contains("data_availability_interval start_time must be before end_time")
  }

  @Test
  fun `validate rejects bare measurement consumer ID`() {
    val eventGroup = VALID_EVENT_GROUP.copy { measurementConsumer = "mc-1" }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception)
      .hasMessageThat()
      .contains("expected measurementConsumers/<measurement-consumer-id>")
  }

  @Test
  fun `validate rejects invalid interval timestamp`() {
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

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception).hasMessageThat().contains("start_time must be a valid timestamp")
  }

  @Test
  fun `validate rejects URL-unsafe entity key`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        entityKey = entityKey {
          entityType = "campaign"
          entityId = "campaign/1"
        }
      }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception).hasMessageThat().contains("entity_id must be URL-safe")
  }

  @Test
  fun `validate rejects unspecified media type`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy { mediaTypes += EventGroup.MediaType.MEDIA_TYPE_UNSPECIFIED }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception).hasMessageThat().contains("must not contain MEDIA_TYPE_UNSPECIFIED")
  }

  @Test
  fun `validate rejects missing campaign metadata`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy { eventGroupMetadata = metadata { adMetadata = adMetadata {} } }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception).hasMessageThat().contains("campaign_metadata must be set")
  }

  @Test
  fun `validate rejects client account reference ID longer than 36 characters`() {
    val eventGroup =
      VALID_EVENT_GROUP.copy {
        measurementConsumer = ""
        clientAccountReferenceId = "a".repeat(37)
      }

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception)
      .hasMessageThat()
      .contains("client_account_reference_id must not exceed 36 characters")
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

    val exception =
      assertFailsWith<IllegalStateException> { EventGroupValidator.validate(eventGroup) }

    assertThat(exception).hasMessageThat().contains("Malformed measurement_consumer")
  }

  private companion object {
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
