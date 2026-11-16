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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
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

/** A replacement alias shares cutover publication without creating another labeling path. */
internal class VidLabelingCutoverAliasIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      errorIfGapsExist = false,
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
      modelLineMap = mapOf(MEMOIZED_MODEL_LINE to listOf(DEV_MODEL_LINE)),
    )
  ) {
  @Test
  fun `replacement output publishes cutover and alias without alias jobs or metadata`() =
    runBlocking<Unit> {
      uploadDay(1)
      uploadDay(2)
      assertPublishedThrough(2, replacementAvailable = false)

      val third = uploadDay(3)
      recoverPending()
      assertCompletedForBothPaths(third)
      assertPublishedThrough(3)
      assertThat(listAvailabilityTasks(third.name).map { it.cmmsModelLine })
        .containsExactly(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
      assertThat(listMetadata().map { it.modelLine }.toSet())
        .containsExactly(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
      assertAvailabilityTaskIdentities(third, date(3))

      val fourth = uploadDay(4)
      recoverPending()
      assertCompletedForBothPaths(fourth)
      assertPublishedThrough(4)
    }

  @Test
  fun `replacement alias waits for cutover gap recovery without relabeling completed output`() =
    runBlocking<Unit> {
      for (day in 1..4) {
        uploadDay(day)
        recoverPending()
      }
      assertPublishedThrough(4)
      val publicationCount = dataProvidersService.requests.size
      val previousIntervals = dataProvidersService.requests.last().dataAvailabilityIntervalsList

      val sixth = uploadDay(6)
      assertThat(listModelLines(sixth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertThat(dataProvidersService.requests).hasSize(publicationCount)
      assertThat(dataProvidersService.requests.last().dataAvailabilityIntervalsList)
        .isEqualTo(previousIntervals)
      val artifacts = snapshotOutputArtifacts(listMetadata())

      val fifth = uploadDay(5)
      recoverPending()
      assertThat(listModelLines(sixth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listModelLines(fifth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertPublishedThrough(6)
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
      assertThat(listMetadata().map { it.modelLine }.toSet())
        .containsExactly(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
      assertThat(listAvailabilityTasks(sixth.name).map { it.cmmsModelLine })
        .containsExactly(MEMOIZED_MODEL_LINE)
    }

  private fun date(day: Int): LocalDate = EVENT_DATE.plusDays(day - 1L)

  private suspend fun uploadDay(day: Int): RawImpressionUpload {
    if (date(day).isAfter(today.get())) setToday(date(day))
    val folder = "cutover-alias-day-$day"
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

  private suspend fun assertPublishedThrough(day: Int, replacementAvailable: Boolean = true) {
    val intervals =
      dataProvidersService.requests.last().dataAvailabilityIntervalsList.associate {
        it.key to it.value
      }
    assertThat(intervals.keys)
      .containsExactlyElementsIn(
        if (replacementAvailable) listOf(EXTERNAL_MODEL_LINE, DEV_MODEL_LINE)
        else listOf(EXTERNAL_MODEL_LINE)
      )
    val expectedEnd = date(day).plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant()
    assertThat(intervals.getValue(EXTERNAL_MODEL_LINE).startTime.toInstant())
      .isEqualTo(EVENT_DATE.atStartOfDay(java.time.ZoneOffset.UTC).toInstant())
    assertThat(intervals.getValue(EXTERNAL_MODEL_LINE).endTime.toInstant()).isEqualTo(expectedEnd)
    if (replacementAvailable) {
      val replacementMetadata = listMetadata().filter { it.modelLine == MEMOIZED_MODEL_LINE }
      assertThat(replacementMetadata).isNotEmpty()
      assertThat(intervals.getValue(DEV_MODEL_LINE).startTime)
        .isEqualTo(
          replacementMetadata.minBy { it.interval.startTime.toInstant() }.interval.startTime
        )
      assertThat(intervals.getValue(DEV_MODEL_LINE).endTime)
        .isEqualTo(replacementMetadata.maxBy { it.interval.endTime.toInstant() }.interval.endTime)
    }
  }

  companion object {
    private const val EXTERNAL_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/external"
    private const val DEV_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/dev"
  }
}
