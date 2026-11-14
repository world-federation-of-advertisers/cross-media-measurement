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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import java.time.Duration
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine

/** Historical input is required through D4; the replacement alone supplies D5 and later. */
internal class VidLabelingCutoverGapRecoveryIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      errorIfGapsExist = true,
      redeliveryDelayMillis = 1_000L,
      maxDeliveryAttempts = 5,
      initialToday = EVENT_DATE,
      dispatcherClockUsesToday = true,
      modelLineActiveEnds =
        mapOf(
          DIRECT_MODEL_LINE to
            EVENT_DATE.plusDays(4).atStartOfDay(java.time.ZoneOffset.UTC).toInstant()
        ),
      modelLineActiveStarts =
        mapOf(
          MEMOIZED_MODEL_LINE to
            EVENT_DATE.plusDays(2).atStartOfDay(java.time.ZoneOffset.UTC).toInstant()
        ),
      modelLineCutovers =
        listOf(
          ModelLineCutover(
            externalModelLine = EXTERNAL_MODEL_LINE,
            historicalModelLine = DIRECT_MODEL_LINE,
            replacementModelLine = MEMOIZED_MODEL_LINE,
            cutoverDate = EVENT_DATE.plusDays(2),
            legacyRequiredThroughDate = EVENT_DATE.plusDays(3),
            maximumReportDuration = Duration.ofDays(1),
          )
        ),
    )
  ) {
  @Test
  fun `future cutover does not block historical publication and late replacement day recovers`() =
    runBlocking<Unit> {
      for (day in 1..2) {
        val upload = uploadDay(day)
        assertThat(
            listModelLines(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }.state
          )
          .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      }
      assertExternalEnd(date(2).plusDays(1))
      for (day in 3..4) {
        uploadDay(day)
        recoverPending()
      }
      assertExternalEnd(date(4).plusDays(1))
      val later = uploadDay(6)
      assertThat(listModelLines(later.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertExternalEnd(date(4).plusDays(1))
      val artifacts = snapshotOutputArtifacts(listMetadata())
      val fifth = uploadDay(5)
      recoverPending()
      assertThat(listModelLines(later.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listModelLines(fifth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertExternalEnd(date(6).plusDays(1))
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
    }

  @Test
  fun `cutover missing overlap day blocks until both contributors finish metadata sync`() =
    runBlocking<Unit> {
      uploadDay(1)
      uploadDay(2)
      val fourth = uploadDay(4)
      assertThat(listModelLines(fourth.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertExternalEnd(date(2).plusDays(1))
      uploadDay(3)
      recoverPending()
      assertCompletedForBothPaths(fourth)
      assertExternalEnd(date(4).plusDays(1))
    }

  private fun date(day: Int): LocalDate = EVENT_DATE.plusDays(day - 1L)

  private suspend fun uploadDay(day: Int): RawImpressionUpload {
    // Late provider dates keep the dispatcher and workers at the current date.
    if (date(day).isAfter(today.get())) setToday(date(day))
    val folder = "cutover-day-$day"
    writeRawFile(folder, "input.parquet", listOf("person-$day"), date(day))
    val generation = finalizeRawUpload(folder)
    awaitPipelineIdle(allowFailures = true)
    return listUploads().single { it.doneBlobGeneration == generation }
  }

  private suspend fun recoverPending() {
    repeat(6) {
      assertThat(buildVidLabelingMonitor().runDispatch().dispatchError).isFalse()
      awaitPipelineIdle(allowFailures = true)
    }
  }

  private fun assertExternalEnd(exclusiveEnd: LocalDate) {
    val intervals =
      dataProvidersService.requests.last().dataAvailabilityIntervalsList.associate {
        it.key to it.value
      }
    assertThat(intervals.keys).containsExactly(EXTERNAL_MODEL_LINE)
    assertThat(intervals.getValue(EXTERNAL_MODEL_LINE).endTime.toInstant())
      .isEqualTo(exclusiveEnd.atStartOfDay(java.time.ZoneOffset.UTC).toInstant())
  }

  companion object {
    private const val EXTERNAL_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/external"
  }
}
