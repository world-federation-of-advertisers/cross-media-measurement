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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.measurement.integration.k8s.testing.HighOverlapImpressionRow
import org.measurement.integration.k8s.testing.highOverlapImpressionRow
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.display
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.video
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

@RunWith(JUnit4::class)
class HighOverlapImpressionRowMapperTest {
  private val mapper = HighOverlapImpressionRowMapper(MODEL_LINE, SPEC_FINGERPRINT)

  @Test
  fun `map populates video impression fields`() {
    val row = mapRow(VIDEO_EVENT)

    assertThat(row)
      .isEqualTo(
        highOverlapImpressionRow {
          modelLine = MODEL_LINE
          specFingerprint = SPEC_FINGERPRINT
          edpName = EDP_NAME
          logicalGroup = LOGICAL_GROUP
          entityType = "campaign"
          entityId = "test-campaign"
          cmmsEventGroupReferenceId = "campaign-test-campaign"
          vid = VID
          eventTime = 1_234_567_890L
          gender = "FEMALE"
          ageGroup = "YEARS_35_TO_54"
          usState = "CALIFORNIA"
          signedIn = true
          videoCompletedFraction = 0.75F
          videoViewableFraction = 0.5F
          displayViewableFraction = 0.0F
          mediaType = "VIDEO"
        }
      )
  }

  @Test
  fun `map populates display impression fields`() {
    val event = testEvent {
      common = VIDEO_EVENT.common
      display = display { viewableFraction = 0.25F }
    }

    val row = mapRow(event)

    assertThat(row.mediaType).isEqualTo("DISPLAY")
    assertThat(row.displayViewableFraction).isEqualTo(0.25F)
    assertThat(row.videoCompletedFraction).isEqualTo(0.0F)
    assertThat(row.videoViewableFraction).isEqualTo(0.0F)
  }

  @Test
  fun `map identifies video with zero engagement fractions`() {
    val row = mapRow(VIDEO_EVENT.copy { video = video {} })

    assertThat(row.mediaType).isEqualTo("VIDEO")
    assertThat(row.videoCompletedFraction).isEqualTo(0.0F)
    assertThat(row.videoViewableFraction).isEqualTo(0.0F)
  }

  @Test
  fun `map identifies display with zero viewable fraction`() {
    val row = mapRow(testEvent { display = display {} })

    assertThat(row.mediaType).isEqualTo("DISPLAY")
    assertThat(row.displayViewableFraction).isEqualTo(0.0F)
  }

  @Test
  fun `map truncates timestamp precision below microseconds`() {
    val row = mapRow(VIDEO_EVENT, Instant.ofEpochSecond(0, 999))

    assertThat(row.eventTime).isEqualTo(0L)
  }

  @Test
  fun `map converts timestamps before the epoch to microseconds`() {
    val row = mapRow(VIDEO_EVENT, Instant.ofEpochSecond(-1, 123_456_789))

    assertThat(row.eventTime).isEqualTo(-876_544L)
  }

  @Test
  fun `map preserves unspecified demographic enum names`() {
    val row = mapRow(testEvent { video = video {} })

    assertThat(row.gender).isEqualTo("GENDER_UNSPECIFIED")
    assertThat(row.ageGroup).isEqualTo("AGE_GROUP_UNSPECIFIED")
    assertThat(row.usState).isEqualTo("US_STATE_UNSPECIFIED")
    assertThat(row.signedIn).isFalse()
  }

  @Test
  fun `map rejects impressions with both video and display`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        mapRow(VIDEO_EVENT.copy { display = display { viewableFraction = 0.25F } })
      }

    assertThat(exception.message).contains("exactly one")
  }

  @Test
  fun `map rejects impressions without video or display`() {
    val exception = assertFailsWith<IllegalArgumentException> { mapRow(testEvent {}) }

    assertThat(exception.message).contains("exactly one")
  }

  private fun mapRow(
    message: TestEvent,
    timestamp: Instant = EVENT_TIME,
  ): HighOverlapImpressionRow {
    return mapper.map(EDP_NAME, LOGICAL_GROUP, ENTITY_KEY, LabeledEvent(timestamp, VID, message))
  }

  companion object {
    private const val MODEL_LINE =
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelLines/AAAAAAAAAHs"
    private const val SPEC_FINGERPRINT = "test-fingerprint"
    private const val EDP_NAME = "test-edp"
    private const val LOGICAL_GROUP = "test-group"
    private const val VID = 987L
    private val ENTITY_KEY = EntityKey(entityType = "campaign", entityId = "test-campaign")
    private val EVENT_TIME = Instant.ofEpochSecond(1234, 567_890_123)
    private val VIDEO_EVENT = testEvent {
      common = common {
        gender = Common.Gender.FEMALE
        ageGroup = Common.AgeGroup.YEARS_35_TO_54
        usState = Common.UsState.CALIFORNIA
        signedIn = true
      }
      video = video {
        completedFraction = 0.75F
        viewableFraction = 0.5F
      }
    }
  }
}
