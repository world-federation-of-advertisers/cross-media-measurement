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
import com.google.protobuf.ByteString
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys

internal class VidLabelingMonitorIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `raw input monitor classifies findings and resolves a later valid done marker`() =
    runBlocking {
      val registeredInput =
        writeRawFile("registered-missing", "input.parquet", listOf("registered-person"))
      finalizeRawUpload("registered-missing")
      awaitPipelineIdle()
      checkNotNull(fileStorage.getBlob(blobKey(registeredInput))).delete()

      writeRawFile("quiet", "input.parquet", listOf("quiet-person"))
      fileStorage.writeBlob("$rootKey/raw/empty/done", flowOf(ByteString.EMPTY))

      fileStorage.writeBlob("$rootKey/raw/late/done", flowOf(ByteString.EMPTY))
      delay(5)
      writeRawFile("late", "input.parquet", listOf("late-person"))

      writeRawFile("overlap/child", "input.parquet", listOf("overlap-person"))
      fileStorage.writeBlob("$rootKey/raw/overlap/done", flowOf(ByteString.EMPTY))
      fileStorage.writeBlob("$rootKey/raw/overlap/child/done", flowOf(ByteString.EMPTY))

      fileStorage.writeBlob("$rootKey/raw/control/heartbeat", flowOf(ByteString.EMPTY))
      fileStorage.writeBlob("$rootKey/output/ignored", flowOf(ByteString.copyFromUtf8("output")))
      fileStorage.writeBlob("$rootKey/rank/ignored", flowOf(ByteString.copyFromUtf8("rank")))
      fileStorage.writeBlob("$rootKey/models/ignored", flowOf(ByteString.copyFromUtf8("model")))
      fileStorage.writeBlob("$rootKey/tmp/ignored", flowOf(ByteString.copyFromUtf8("temporary")))

      val monitor = buildVidLabelingMonitor()
      val before = monitor.runHealth()

      assertThat(before.dataQualityCheckFailed).isFalse()
      assertThat(before.missingDoneBlobs).isAtLeast(1L)
      assertThat(before.zeroImpressionDates).isAtLeast(1L)
      assertThat(before.lateArrivingFiles).isAtLeast(1L)
      assertThat(before.unregisteredDoneBlobs).isAtLeast(1L)
      assertThat(before.ambiguousDoneMarkerLayouts).isAtLeast(1L)
      assertThat(before.missingRawFiles).isEqualTo(1L)

      finalizeRawUpload("quiet")
      awaitPipelineIdle()
      val after = monitor.runHealth()

      assertThat(after.dataQualityCheckFailed).isFalse()
      assertThat(after.missingDoneBlobs).isLessThan(before.missingDoneBlobs)
      assertThat(after.missingRawFiles).isEqualTo(1L)
    }

  @Test
  fun `monitor accepts a no-op done rewrite and reports missing labeled output without mutation`() =
    runBlocking {
      writeRawFile("monitor-no-op", "input.parquet", listOf("person"))
      finalizeRawUpload("monitor-no-op")
      awaitPipelineIdle()

      finalizeRawUpload("monitor-no-op")
      workItemTransport.awaitIdle()
      assertThat(listUploads()).hasSize(1)

      val directDoneKey =
        blobKey(LabeledImpressionsBlobKeys.forDoneUri(outputPrefix, DIRECT_MODEL_LINE, EVENT_DATE))
      checkNotNull(fileStorage.getBlob(directDoneKey)).delete()
      val before =
        listOf(
          listUploads().toString(),
          listModelLines(listUploads().single().name).toString(),
          listMetadata(showDeleted = true).toString(),
          poolAssignmentWorkItems().toString(),
          rankBuilderWorkItems().toString(),
          vidLabelerWorkItems().toString(),
          listAvailabilityTasks(listUploads().single().name).toString(),
        )

      val result = buildVidLabelingMonitor().runHealth()

      assertThat(result.dataQualityCheckFailed).isFalse()
      assertThat(result.unregisteredDoneBlobs).isEqualTo(0L)
      assertThat(result.missingLabeledOutputs).isEqualTo(1L)
      val after =
        listOf(
          listUploads().toString(),
          listModelLines(listUploads().single().name).toString(),
          listMetadata(showDeleted = true).toString(),
          poolAssignmentWorkItems().toString(),
          rankBuilderWorkItems().toString(),
          vidLabelerWorkItems().toString(),
          listAvailabilityTasks(listUploads().single().name).toString(),
        )
      assertThat(after).isEqualTo(before)
    }
}
