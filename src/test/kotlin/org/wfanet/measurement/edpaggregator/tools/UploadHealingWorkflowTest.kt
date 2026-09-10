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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.CreateUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GetUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

@RunWith(JUnit4::class)
class UploadHealingWorkflowTest {
  private val operationsService = InMemoryOperationsService()
  private val uploadsByName = mutableMapOf<String, RawImpressionUpload>()
  private val modelLinesByUpload = mutableMapOf<String, List<RawImpressionUploadModelLine>>()
  private val uploadsService =
    object : RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase() {
      override suspend fun getRawImpressionUpload(
        request: GetRawImpressionUploadRequest
      ): RawImpressionUpload = uploadsByName.getValue(request.name)

      override suspend fun listRawImpressionUploads(request: ListRawImpressionUploadsRequest) =
        listRawImpressionUploadsResponse {
          rawImpressionUploads +=
            uploadsByName.values.filter {
              it.name.startsWith(request.parent) &&
                (request.filter.doneBlobUri.isEmpty() ||
                  it.doneBlobUri == request.filter.doneBlobUri)
            }
        }
    }
  private val modelLinesService =
    object :
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase() {
      override suspend fun listRawImpressionUploadModelLines(
        request: ListRawImpressionUploadModelLinesRequest
      ) = listRawImpressionUploadModelLinesResponse {
        val rows =
          if (request.parent.endsWith("/rawImpressionUploads/-")) {
            modelLinesByUpload.values.flatten()
          } else {
            modelLinesByUpload[request.parent].orEmpty()
          }
        rawImpressionUploadModelLines +=
          rows.filter {
            (request.filter.cmmsModelLine.isEmpty() ||
              it.cmmsModelLine == request.filter.cmmsModelLine) &&
              (request.filter.stateInList.isEmpty() || it.state in request.filter.stateInList)
          }
      }
    }
  private val activeSnapshots = mutableSetOf<Pair<String, String>>()
  private val rankIndexBlobsService =
    object : RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase() {
      override suspend fun listRankIndexBlobs(request: ListRankIndexBlobsRequest) =
        listRankIndexBlobsResponse {
          if (request.parent to request.filter.cmmsModelLine in activeSnapshots) {
            rankIndexBlobs += rankIndexBlob { name = "${request.parent}/rankIndexBlobs/snapshot" }
          }
        }
    }
  private val recoveredUploads = mutableListOf<String>()
  private val rawImpressionUploadFileService:
    RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase =
    mockService()
  private val impressionMetadataService:
    ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(operationsService)
    addService(uploadsService)
    addService(modelLinesService)
    addService(rankIndexBlobsService)
    addService(rawImpressionUploadFileService)
    addService(impressionMetadataService)
  }

  @Test
  fun `resume checkpoints eviction and advances replacements oldest first`() = runBlocking {
    for ((index, uploadName) in listOf(D1, D2, D3, D4, D5).withIndex()) {
      uploadsByName[uploadName] = sourceUpload(uploadName, index)
      modelLinesByUpload[uploadName] =
        listOf(
          rawImpressionUploadModelLine {
            name = "$uploadName/rawImpressionUploadModelLines/ml"
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.COMPLETED
          }
        )
      activeSnapshots += uploadName to MODEL_LINE
    }
    val channel = grpcTestServerRule.channel
    val plan =
      EvictUploader(
          RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel),
          RawImpressionUploadModelLineServiceGrpcKt
            .RawImpressionUploadModelLineServiceCoroutineStub(channel),
          RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(channel),
          RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(channel),
          ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(channel),
          "gs://output/vid",
          deleteBlob = { true },
        )
        .plan(listOf(D2, D4), cutoffTime = Instant.EPOCH)
    assertThat(plan.cascade.map { it.uploadName }).containsExactly(D2, D3, D4, D5).inOrder()
    assertThat(plan.cascade.map { it.recoveryPredecessorUploadName })
      .containsExactly(D1, D2, D3, D4)
      .inOrder()

    val evictedEntries = mutableListOf<String>()
    val workflow =
      UploadHealingWorkflow(
        UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(
          grpcTestServerRule.channel
        ),
        RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
          grpcTestServerRule.channel
        ),
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
          grpcTestServerRule.channel
        ),
        RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(grpcTestServerRule.channel),
        EvictionExecutor { plan, _, checkpoint ->
          for (entry in plan.cascade) {
            evictedEntries += entry.modelLineName
            checkpoint(entry)
          }
          EvictUploader.EvictionResult(plan.cascade.map { it.modelLineName }, 4, 4, 8)
        },
        RecoveryExecutor { source, modelLines ->
          recoveredUploads += source
          RecoverUploader.Result(
            source,
            uploadsByName.getValue(source).doneBlobUri,
            999L,
            modelLines,
          )
        },
      )

    val started = workflow.start(plan, "bad source data", "gs://output/vid")

    assertThat(evictedEntries).containsExactly(M2, M3, M4, M5).inOrder()
    assertThat(started.operation.stepsList)
      .comparingElementsUsing(UploadHealingStepStateCorrespondence)
      .containsExactlyElementsIn(List(4) { UploadHealingStep.State.WAITING_FOR_REPLACEMENT })
    assertThat(started.nextAction).contains(D2)

    addCompletedReplacement(D2, D2_REPLACEMENT)
    val afterD2 = workflow.resume(started.operation.name)

    assertThat(recoveredUploads).containsExactly(D3)
    assertThat(afterD2.nextAction).contains(D3)
    assertThat(
        afterD2.operation.stepsList
          .single { it.sourceRawImpressionUpload == D3 }
          .recoveryDoneBlobGeneration
      )
      .isEqualTo(999L)

    val whileD3IsRunning = workflow.resume(started.operation.name)

    assertThat(whileD3IsRunning.nextAction).contains(D3)
    assertThat(recoveredUploads).containsExactly(D3)

    addCompletedReplacement(D3, D3_REPLACEMENT)
    val afterD3 = workflow.resume(started.operation.name)

    assertThat(afterD3.nextAction).contains(D4)
    assertThat(recoveredUploads).containsExactly(D3)

    addCompletedReplacement(D4, D4_REPLACEMENT)
    val afterD4 = workflow.resume(started.operation.name)

    assertThat(afterD4.nextAction).contains(D5)
    assertThat(recoveredUploads).containsExactly(D3, D5).inOrder()

    addCompletedReplacement(D5, D5_REPLACEMENT)
    val completed = workflow.resume(started.operation.name)

    assertThat(completed.operation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    Unit
  }

  private fun addCompletedReplacement(sourceName: String, replacementName: String) {
    val source = uploadsByName.getValue(sourceName)
    uploadsByName[replacementName] = rawImpressionUpload {
      name = replacementName
      state = RawImpressionUpload.State.COMPLETED
      doneBlobUri = source.doneBlobUri
      doneBlobGeneration = source.doneBlobGeneration + 10L
      doneBlobCreateTime = source.doneBlobCreateTime.toBuilder().setSeconds(100L).build()
      replacesRawImpressionUpload = sourceName
      registrationComplete = true
    }
    modelLinesByUpload[replacementName] =
      listOf(
        rawImpressionUploadModelLine {
          name = "$replacementName/rawImpressionUploadModelLines/replacement"
          cmmsModelLine = MODEL_LINE
          state = RawImpressionUploadModelLine.State.COMPLETED
        }
      )
    activeSnapshots += replacementName to MODEL_LINE
  }

  private fun sourceUpload(name: String, index: Int): RawImpressionUpload = rawImpressionUpload {
    this.name = name
    state = RawImpressionUpload.State.FAILED
    doneBlobUri = "gs://input/day-$index/done"
    doneBlobGeneration = index.toLong() + 1L
    doneBlobCreateTime = Instant.ofEpochSecond(index.toLong() + 1L).toProtoTime()
    createTime = Instant.ofEpochSecond(index.toLong() + 1L).toProtoTime()
    registrationComplete = true
  }

  private class InMemoryOperationsService :
    UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase() {
    private var operation: UploadHealingOperation? = null
    private var etagSequence = 0

    override suspend fun createUploadHealingOperation(
      request: CreateUploadHealingOperationRequest
    ): UploadHealingOperation {
      operation?.let {
        return it
      }
      val operationName =
        "${request.parent}/uploadHealingOperations/${request.uploadHealingOperationId}"
      operation =
        request.uploadHealingOperation.copy {
          name = operationName
          state = UploadHealingOperation.State.IN_PROGRESS
          steps.clear()
          steps +=
            request.uploadHealingOperation.stepsList.mapIndexed { index, step ->
              step.copy {
                name = "$operationName/steps/${index + 1}"
                state = UploadHealingStep.State.PENDING_EVICTION
                etag = "etag-${++etagSequence}"
              }
            }
        }
      return operation!!
    }

    override suspend fun getUploadHealingOperation(
      request: GetUploadHealingOperationRequest
    ): UploadHealingOperation = requireNotNull(operation)

    override suspend fun advanceUploadHealingStep(
      request: AdvanceUploadHealingStepRequest
    ): UploadHealingStep {
      val current = requireNotNull(operation)
      val updatedSteps =
        current.stepsList.map { step ->
          if (step.name != request.name) {
            step
          } else {
            step.copy {
              state =
                when (request.action) {
                  AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION ->
                    if (step.recoveryTarget) {
                      UploadHealingStep.State.WAITING_FOR_REPLACEMENT
                    } else {
                      UploadHealingStep.State.COMPLETE
                    }
                  AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY ->
                    UploadHealingStep.State.RECOVERY_STARTED
                  AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT ->
                    UploadHealingStep.State.COMPLETE
                  AdvanceUploadHealingStepRequest.Action.ACTION_UNSPECIFIED,
                  AdvanceUploadHealingStepRequest.Action.UNRECOGNIZED -> error("action is required")
                }
              replacementRawImpressionUpload = request.replacementRawImpressionUpload
              recoveryDoneBlobGeneration = request.recoveryDoneBlobGeneration
              etag = "etag-${++etagSequence}"
            }
          }
        }
      operation =
        current.copy {
          steps.clear()
          steps += updatedSteps
          if (updatedSteps.all { it.state == UploadHealingStep.State.COMPLETE }) {
            state = UploadHealingOperation.State.COMPLETE
          }
        }
      return operation!!.stepsList.single { it.name == request.name }
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val D1 = "$DATA_PROVIDER/rawImpressionUploads/d1"
    private const val D2 = "$DATA_PROVIDER/rawImpressionUploads/d2"
    private const val D3 = "$DATA_PROVIDER/rawImpressionUploads/d3"
    private const val D4 = "$DATA_PROVIDER/rawImpressionUploads/d4"
    private const val D5 = "$DATA_PROVIDER/rawImpressionUploads/d5"
    private const val D2_REPLACEMENT = "$DATA_PROVIDER/rawImpressionUploads/d2-replacement"
    private const val D3_REPLACEMENT = "$DATA_PROVIDER/rawImpressionUploads/d3-replacement"
    private const val D4_REPLACEMENT = "$DATA_PROVIDER/rawImpressionUploads/d4-replacement"
    private const val D5_REPLACEMENT = "$DATA_PROVIDER/rawImpressionUploads/d5-replacement"
    private const val M2 = "$D2/rawImpressionUploadModelLines/ml"
    private const val M3 = "$D3/rawImpressionUploadModelLines/ml"
    private const val M4 = "$D4/rawImpressionUploadModelLines/ml"
    private const val M5 = "$D5/rawImpressionUploadModelLines/ml"
    private val UploadHealingStepStateCorrespondence =
      com.google.common.truth.Correspondence.transforming<
        UploadHealingStep,
        UploadHealingStep.State,
      >(
        { it.state },
        "has state",
      )
  }
}
