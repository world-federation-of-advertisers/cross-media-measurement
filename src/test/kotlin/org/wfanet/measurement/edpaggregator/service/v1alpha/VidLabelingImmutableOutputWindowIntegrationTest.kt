/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import java.time.ZoneOffset
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams

internal class VidLabelingImmutableOutputWindowIntegrationTest :
  VidLabelingPipelineTestHarness(activeWindowConfig()) {
  @Test
  fun `closed and future lines are skipped while the open ended line completes`() = runBlocking {
    writeRawFile("windows", "input.parquet", listOf("person"))
    val generation = finalizeRawUpload("windows")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    val rows = listModelLines(upload.name)
    assertThat(rows.map { it.cmmsModelLine }).containsExactly(OPEN_MODEL_LINE)
    assertThat(rows.single().state).isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(listMetadata().map { it.modelLine }).containsExactly(OPEN_MODEL_LINE)
    assertThat(listAvailabilityTasks(upload.name).map { it.cmmsModelLine })
      .containsExactly(OPEN_MODEL_LINE)
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(readLabeledPeople(OPEN_MODEL_LINE).map { it.personId }).containsExactly("person")
    Unit
  }

  @Test
  fun `fully filtered direct input commits explicit empty scope and completes availability`() =
    runBlocking<Unit> {
      setVisibleModelLines(OPEN_MODEL_LINE)
      withholdNextVidLabelerPublications()
      writeRawFile("empty-direct-window", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("empty-direct-window")
      workItemTransport.awaitIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      val work = vidLabelerWorkItems().single()
      val outer = work.workItemParams.unpack(WorkItemParams::class.java)
      val params = outer.appParams.unpack(VidLabelerParams::class.java)
      val config =
        params
          .getModelLineConfigsOrThrow(OPEN_MODEL_LINE)
          .toBuilder()
          .setActiveStartTime(
            EVENT_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
          )
          .build()
      val modified = params.toBuilder().putModelLineConfigs(OPEN_MODEL_LINE, config).build()
      runVidLabelerDirect(
        work
          .toBuilder()
          .setWorkItemParams(outer.toBuilder().setAppParams(modified.pack()).build().pack())
          .build()
      )
      awaitPipelineIdle()
      val job =
        VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
          .getVidLabelingJob(getVidLabelingJobRequest { name = params.vidLabelingJob })
      assertThat(job.state).isEqualTo(VidLabelingJob.State.SUCCEEDED)
      val records =
        OutputManifestStore(
            fileStorage,
            params.vidLabeledImpressionsStorageParams.impressionsBlobPrefix,
          )
          .read(job.outputManifest)
          .toList()
      assertThat(
          records.filter { it.hasFileCompleted() }.map { it.fileCompleted.rawImpressionUploadFile }
        )
        .containsExactlyElementsIn(job.rawImpressionUploadFilesList)
      assertThat(records.any { it.hasLabeledBlob() }).isFalse()
      assertThat(listMetadata()).isEmpty()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listAvailabilityTasks(upload.name).single().state)
        .isEqualTo(WorkItem.State.SUCCEEDED)
    }
}

private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
private const val CLOSED_MODEL_LINE = "$MODEL_SUITE/modelLines/closed"
private const val OPEN_MODEL_LINE = "$MODEL_SUITE/modelLines/open"
private const val FUTURE_MODEL_LINE = "$MODEL_SUITE/modelLines/future"

private fun activeWindowConfig(): PipelineHarnessConfig {
  val now = EVENT_DATE.plusDays(2).atStartOfDay(ZoneOffset.UTC).toInstant()
  val fixtures =
    listOf(
      ModelLineFixture(
        CLOSED_MODEL_LINE,
        "$MODEL_SUITE/modelReleases/closed",
        memoized = false,
        activeStart = now.minusSeconds(200),
        activeEnd = now,
      ),
      ModelLineFixture(
        OPEN_MODEL_LINE,
        "$MODEL_SUITE/modelReleases/open",
        memoized = false,
        activeStart = EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
      ),
      ModelLineFixture(
        FUTURE_MODEL_LINE,
        "$MODEL_SUITE/modelReleases/future",
        memoized = false,
        activeStart = now.plusSeconds(100),
      ),
    )
  return PipelineHarnessConfig(
    immutableOutputPublication = true,
    extraModelLines = fixtures,
    initiallyVisibleModelLines = fixtures.mapTo(mutableSetOf()) { it.name },
  )
}
