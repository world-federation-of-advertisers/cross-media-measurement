/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams

internal class VidLabelingPoolFinalizerRecoveryIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      numberOfShards = 2,
      immutableOutputPublication = true,
      initiallyVisibleModelLines = setOf(MEMOIZED_MODEL_LINE),
    )
  ) {
  private fun params(work: WorkItem) =
    work.workItemParams
      .unpack(WorkItemParams::class.java)
      .appParams
      .unpack(SubpoolAssignerParams::class.java)

  private suspend fun prepare(folder: String): Pair<List<WorkItem>, String> {
    withholdNextPoolAssignmentPublications(2)
    val extractor = EventIdDigestExtractor()
    val event =
      generateSequence(0) { it + 1 }
        .map { "shard-zero-$it" }
        .first { Math.floorMod(extractor.extract(ByteString.copyFromUtf8(it)).high, 2L) == 0L }
    writeRawEvents(folder, "input.parquet", listOf(RawEventFixture(event, "person", EVENT_DATE)))
    val generation = finalizeRawUpload(folder)
    workItemTransport.awaitIdle()
    return poolAssignmentWorkItems().sortedBy { params(it).shardIndex } to
      listUploads().single { it.doneBlobGeneration == generation }.name
  }

  @Test
  fun `a non-final completed child recovers the selected merge after an empty final shard loses its transition`() =
    runBlocking<Unit> {
      val (works, upload) = prepare("arbitrary-child-recovery")
      withholdNextRankBuilderPublications()
      runPoolAssignerDirect(works[0])
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/MarkRawImpressionUploadModelLineRanking"
      )
      assertThat(runCatching { runPoolAssignerDirect(works[1]) }.isFailure).isTrue()
      val stub = PoolAssignmentJobServiceCoroutineStub(edpaPublicServer.channel)
      val first =
        stub.getPoolAssignmentJob(
          getPoolAssignmentJobRequest { name = params(works[0]).poolAssignmentJob }
        )
      val last =
        stub.getPoolAssignmentJob(
          getPoolAssignmentJobRequest { name = params(works[1]).poolAssignmentJob }
        )
      assertThat(first.lastShard).isFalse()
      assertThat(last.lastShard).isTrue()
      assertThat(last.hasEncryptedDek()).isFalse()
      val parent = listModelLines(upload).single()
      assertThat(parent.encryptedMergedDek).isEqualTo(first.encryptedDek)
      val selected = parent.mergedOutputManifest
      assertThat(selected.blobGeneration).isGreaterThan(0L)
      // This is exactly the minimum-index child chosen by the existing monitor.
      runPoolAssignerDirect(works[0])
      assertThat(listModelLines(upload).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.RANKING)
      assertThat(listModelLines(upload).single().mergedOutputManifest).isEqualTo(selected)
      republishQueuedRankBuilders()
      awaitPipelineIdle()
      assertThat(listModelLines(upload).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("person")
    }

  @Test
  fun `completed legacy empty groups gain an explicit merge selection and complete labeling`() =
    runBlocking<Unit> {
      val (works, upload) = prepare("legacy-empty-recovery")
      val stub = PoolAssignmentJobServiceCoroutineStub(edpaPublicServer.channel)
      val unusedLegacyKey = SubpoolFingerprintsStore(mapStorage, kmsClient).generateDek(kekUri)
      for (work in works) {
        val job =
          stub.getPoolAssignmentJob(
            getPoolAssignmentJobRequest { name = params(work).poolAssignmentJob }
          )
        // Old workers always supplied a DEK, including successful empty shards.
        stub.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            name = job.name
            etag = job.etag
            requestId = UUID.randomUUID().toString()
            encryptedDek = unusedLegacyKey
          }
        )
      }
      runPoolAssignerDirect(works[0])
      awaitPipelineIdle()
      assertThat(listModelLines(upload).single().mergedOutputManifest.blobGeneration)
        .isGreaterThan(0L)
      assertThat(rankBuilderWorkItems()).isEmpty()
      assertThat(listRankIndexBlobs(upload)).isEmpty()
      assertThat(listModelLines(upload).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("person")
    }
}
