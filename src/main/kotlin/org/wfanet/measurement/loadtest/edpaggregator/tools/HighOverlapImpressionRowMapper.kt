/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.edpaggregator.tools

import org.measurement.integration.k8s.testing.HighOverlapImpressionRow
import org.measurement.integration.k8s.testing.highOverlapImpressionRow
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent

/** Maps labeled impressions to BigQuery rows for a high overlap dataset. */
class HighOverlapImpressionRowMapper(
  private val modelLine: String,
  private val specFingerprint: String,
) {
  /**
   * Returns a row with a timestamp in microseconds and media type determined by message presence.
   *
   * @throws IllegalArgumentException if the impression does not contain exactly one of video or
   *   display.
   */
  fun map(
    edpName: String,
    logicalGroup: String,
    entityKey: EntityKey,
    event: LabeledEvent<TestEvent>,
  ): HighOverlapImpressionRow {
    require(event.message.hasVideo() != event.message.hasDisplay()) {
      "High overlap impression must contain exactly one of video or display"
    }
    return highOverlapImpressionRow {
      modelLine = this@HighOverlapImpressionRowMapper.modelLine
      specFingerprint = this@HighOverlapImpressionRowMapper.specFingerprint
      this.edpName = edpName
      this.logicalGroup = logicalGroup
      entityType = entityKey.entityType
      entityId = entityKey.entityId
      cmmsEventGroupReferenceId = "${entityKey.entityType}-${entityKey.entityId}"
      vid = event.vid
      eventTime = event.timestamp.epochSecond * MICROS_PER_SECOND + event.timestamp.nano / 1_000
      gender = event.message.common.gender.name
      ageGroup = event.message.common.ageGroup.name
      usState = event.message.common.usState.name
      signedIn = event.message.common.signedIn
      videoCompletedFraction = event.message.video.completedFraction
      videoViewableFraction = event.message.video.viewableFraction
      displayViewableFraction = event.message.display.viewableFraction
      mediaType = if (event.message.hasVideo()) "VIDEO" else "DISPLAY"
    }
  }

  companion object {
    private const val MICROS_PER_SECOND = 1_000_000L
  }
}
