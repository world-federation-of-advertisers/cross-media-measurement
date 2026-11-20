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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine

/** Native aliases are validated independently while the advertiser cutover is in the future. */
internal class VidLabelingNativeAvailabilityIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      errorIfGapsExist = false,
      initialToday = EVENT_DATE,
      dispatcherClockUsesToday = true,
      modelLineCutovers =
        listOf(
          ModelLineCutover(
            externalModelLine = EXTERNAL_MODEL_LINE,
            historicalModelLine = DIRECT_MODEL_LINE,
            replacementModelLine = MEMOIZED_MODEL_LINE,
            cutoverDate = EVENT_DATE.plusDays(180),
            legacyRequiredThroughDate = EVENT_DATE.plusDays(182),
            maximumReportDuration = Duration.ofDays(2),
          )
        ),
      modelLineMap = mapOf(MEMOIZED_MODEL_LINE to listOf(DEV_MODEL_LINE)),
    )
  ) {
  @Test
  fun `native gap before future cutover preserves comparison prefix and allows legacy advance`() =
    runBlocking<Unit> {
      uploadDay("both-day-1", 1)
      setVisibleModelLines(DIRECT_MODEL_LINE)
      uploadDay("legacy-day-2", 2)
      setVisibleModelLines(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
      val third = uploadDay("both-day-3", 3)

      assertEnds(legacyThrough = 3, nativeThrough = 1)
      assertThat(
          listModelLines(third.name).single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }.state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertThat(listModelLines(third.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }.state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      val artifacts = snapshotOutputArtifacts(listMetadata())

      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      val second = uploadDay("replacement-day-2", 2)
      setVisibleModelLines(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
      repeat(6) {
        assertThat(buildVidLabelingMonitor().runDispatch().dispatchError).isFalse()
        awaitPipelineIdle(allowFailures = true)
      }
      assertEnds(legacyThrough = 3, nativeThrough = 3)
      assertThat(listModelLines(second.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(
          listModelLines(third.name).single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }.state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
    }

  @Test
  fun `comparison advances and completes its source while legacy gap blocks the projection`() =
    runBlocking<Unit> {
      uploadDay("both-day-1", 1)
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      uploadDay("replacement-day-2", 2)
      setVisibleModelLines(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
      val third = uploadDay("both-day-3", 3)

      assertEnds(legacyThrough = 1, nativeThrough = 3)
      assertThat(
          listModelLines(third.name).single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }.state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listModelLines(third.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }.state)
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertThat(listMetadata().map { it.modelLine }.toSet())
        .containsExactly(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)
    }

  private suspend fun uploadDay(folder: String, day: Int): RawImpressionUpload {
    val date = EVENT_DATE.plusDays(day - 1L)
    if (date.isAfter(today.get())) setToday(date)
    writeRawFile(folder, "input.parquet", listOf("person-$day"), date)
    val generation = finalizeRawUpload(folder)
    awaitPipelineIdle(allowFailures = true)
    return listUploads().single {
      it.doneBlobGeneration == generation && it.doneBlobUri.contains(folder)
    }
  }

  private fun assertEnds(legacyThrough: Int, nativeThrough: Int) {
    val intervals =
      dataProvidersService.requests.last().dataAvailabilityIntervalsList.associate {
        it.key to it.value
      }
    assertThat(intervals.keys).containsExactly(EXTERNAL_MODEL_LINE, DEV_MODEL_LINE)
    assertThat(intervals.getValue(EXTERNAL_MODEL_LINE).endTime.toInstant())
      .isEqualTo(
        EVENT_DATE.plusDays(legacyThrough.toLong())
          .atStartOfDay(java.time.ZoneOffset.UTC)
          .toInstant()
      )
    assertThat(intervals.getValue(DEV_MODEL_LINE).endTime.toInstant())
      .isEqualTo(
        EVENT_DATE.plusDays(nativeThrough.toLong())
          .atStartOfDay(java.time.ZoneOffset.UTC)
          .toInstant()
      )
  }

  companion object {
    private const val EXTERNAL_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/external"
    private const val DEV_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/dev"
  }
}
