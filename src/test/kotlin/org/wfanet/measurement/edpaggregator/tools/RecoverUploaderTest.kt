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
import com.google.protobuf.Timestamp
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

@RunWith(JUnit4::class)
class RecoverUploaderTest {
  private val uploadsService:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()
  private val modelLinesService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val rankIndexBlobsService:
    RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(uploadsService)
    addService(modelLinesService)
    addService(rankIndexBlobsService)
  }

  @Test
  fun `recover writes a new done generation for failed memoized model lines`() = runBlocking {
    stubSourceUpload()
    stubModelLine(RawImpressionUploadModelLine.State.FAILED)
    stubSnapshot()
    var rewriteRequest: Triple<String, Long, Map<String, String>>? = null
    val recoverUploader = recoverUploader { uri, generation, metadata ->
      rewriteRequest = Triple(uri, generation, metadata)
      NEW_GENERATION
    }

    val result = recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))

    assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION)
    assertThat(rewriteRequest)
      .isEqualTo(
        Triple(
          DONE_BLOB_URI,
          GENERATION,
          mapOf(
            WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to MODEL_LINE,
            WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to UPLOAD,
            WatchedBlobs.EVICTION_OPERATION_ID_KEY to EVICTION_OPERATION_ID,
          ),
        )
      )
  }

  @Test
  fun `recover rejects a rewrite that returns the source generation`() = runBlocking {
    stubSourceUpload()
    stubModelLine(RawImpressionUploadModelLine.State.FAILED)
    stubSnapshot()
    val recoverUploader = recoverUploader { _, _, _ -> GENERATION }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("distinct valid generation")
  }

  @Test
  fun `recover rejects a superseded upload`() = runBlocking {
    stubSourceUpload(
      rawImpressionUpload {
        name = REPLACEMENT_UPLOAD
        doneBlobUri = DONE_BLOB_URI
        doneBlobGeneration = NEW_GENERATION
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime()
      }
    )
    var rewriteCalled = false
    val recoverUploader = recoverUploader { _, _, _ ->
      rewriteCalled = true
      NEW_GENERATION + 1
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("superseded by $REPLACEMENT_UPLOAD")
    assertThat(rewriteCalled).isFalse()
  }

  @Test
  fun `recover rejects a model line whose snapshot is still active`() = runBlocking {
    stubSourceUpload()
    stubModelLine(RawImpressionUploadModelLine.State.FAILED)
    stubSnapshot(deleted = false)
    val recoverUploader = recoverUploader { _, _, _ -> NEW_GENERATION }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("complete set of FAILED memoized model lines")
  }

  @Test
  fun `recover rejects a partial set of evicted memoized model lines`() = runBlocking {
    stubSourceUpload()
    whenever(modelLinesService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
        >(
          0
        )
      listRawImpressionUploadModelLinesResponse {
        if (request.parent == UPLOAD) {
          for ((id, modelLine) in listOf("rml1" to MODEL_LINE, "rml2" to MODEL_LINE_2)) {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = "$UPLOAD/rawImpressionUploadModelLines/$id"
              cmmsModelLine = modelLine
              state = RawImpressionUploadModelLine.State.FAILED
              failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
              recoveryAction =
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
              evictionOperationId = EVICTION_OPERATION_ID
              recoveryPredecessorRawImpressionUpload = PREDECESSOR_UPLOAD
            }
          }
        } else {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "${request.parent}/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE
            state = RawImpressionUploadModelLine.State.COMPLETED
          }
        }
      }
    }
    whenever(rankIndexBlobsService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val requestedModelLine =
        invocation
          .getArgument<org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest>(0)
          .filter
          .cmmsModelLine
      listRankIndexBlobsResponse {
        rankIndexBlobs += rankIndexBlob {
          name = "$UPLOAD/rankIndexBlobs/${requestedModelLine.substringAfterLast('/')}"
          blobType = RankIndexBlob.BlobType.SNAPSHOT
          cmmsModelLine = requestedModelLine
          deleteTime = Timestamp.getDefaultInstance()
        }
      }
    }
    val recoverUploader = recoverUploader { _, _, _ -> NEW_GENERATION }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("recoverable=[$MODEL_LINE, $MODEL_LINE_2]")
  }

  @Test
  fun `recover rejects a model line that is not failed`() = runBlocking {
    stubSourceUpload()
    stubModelLine(RawImpressionUploadModelLine.State.COMPLETED)
    val recoverUploader = recoverUploader { _, _, _ -> NEW_GENERATION }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("only accepts FAILED model-line rows")
  }

  @Test
  fun `recover rejects an explicitly bad upload that requires EDP correction`() = runBlocking {
    stubSourceUpload()
    stubModelLine(
      RawImpressionUploadModelLine.State.FAILED,
      recoveryAction = RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION,
    )
    stubSnapshot()
    var rewriteCalled = false
    val recoverUploader = recoverUploader { _, _, _ ->
      rewriteCalled = true
      NEW_GENERATION
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("marked for operator recovery")
    assertThat(rewriteCalled).isFalse()
  }

  @Test
  fun `recover rejects an operator recovery before its predecessor replacement completes`() =
    runBlocking {
      stubSourceUpload()
      stubModelLine(
        RawImpressionUploadModelLine.State.FAILED,
        predecessorState = RawImpressionUploadModelLine.State.RANKING,
      )
      stubSnapshot()
      var rewriteCalled = false
      val recoverUploader = recoverUploader { _, _, _ ->
        rewriteCalled = true
        NEW_GENERATION
      }

      val error =
        assertFailsWith<IllegalArgumentException> {
          recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
        }

      assertThat(error).hasMessageThat().contains("has not been replaced by a completed upload")
      assertThat(rewriteCalled).isFalse()
    }

  private fun recoverUploader(
    rewriteDoneBlob: suspend (String, Long, Map<String, String>) -> Long
  ): RecoverUploader {
    val channel = grpcTestServerRule.channel
    return RecoverUploader(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      ),
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(channel),
      rewriteDoneBlob,
    )
  }

  private suspend fun stubSourceUpload(vararg additionalUploads: RawImpressionUpload) {
    val source = rawImpressionUpload {
      name = UPLOAD
      state = RawImpressionUpload.State.FAILED
      doneBlobUri = DONE_BLOB_URI
      doneBlobGeneration = GENERATION
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
    }
    val predecessor = rawImpressionUpload {
      name = PREDECESSOR_UPLOAD
      doneBlobUri = PREDECESSOR_DONE_BLOB_URI
      doneBlobGeneration = GENERATION - 2
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(2).toProtoTime()
    }
    val predecessorReplacement = rawImpressionUpload {
      name = PREDECESSOR_REPLACEMENT_UPLOAD
      doneBlobUri = PREDECESSOR_DONE_BLOB_URI
      doneBlobGeneration = GENERATION - 1
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(1).toProtoTime()
      replacesRawImpressionUpload = PREDECESSOR_UPLOAD
    }
    whenever(uploadsService.getRawImpressionUpload(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
        >(
          0
        )
      when (request.name) {
        UPLOAD -> source
        PREDECESSOR_UPLOAD -> predecessor
        else -> error("Unexpected upload: ${request.name}")
      }
    }
    whenever(uploadsService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
        >(
          0
        )
      listRawImpressionUploadsResponse {
        if (request.filter.doneBlobUri == PREDECESSOR_DONE_BLOB_URI) {
          rawImpressionUploads += predecessor
          rawImpressionUploads += predecessorReplacement
        } else {
          rawImpressionUploads += source
          for (upload in additionalUploads) {
            rawImpressionUploads += upload
          }
        }
      }
    }
  }

  private suspend fun stubModelLine(
    state: RawImpressionUploadModelLine.State,
    predecessorState: RawImpressionUploadModelLine.State =
      RawImpressionUploadModelLine.State.COMPLETED,
    recoveryAction: RawImpressionUploadModelLine.RecoveryAction =
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY,
  ) {
    whenever(modelLinesService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
        >(
          0
        )
      listRawImpressionUploadModelLinesResponse {
        rawImpressionUploadModelLines += rawImpressionUploadModelLine {
          name = "${request.parent}/rawImpressionUploadModelLines/rml1"
          cmmsModelLine = MODEL_LINE
          this.state = if (request.parent == UPLOAD) state else predecessorState
          if (request.parent == UPLOAD) {
            failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
            this.recoveryAction = recoveryAction
            evictionOperationId = EVICTION_OPERATION_ID
            recoveryPredecessorRawImpressionUpload = PREDECESSOR_UPLOAD
          }
        }
      }
    }
  }

  private suspend fun stubSnapshot(deleted: Boolean = true) {
    whenever(rankIndexBlobsService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
        >(
          0
        )
      listRankIndexBlobsResponse {
        rankIndexBlobs += rankIndexBlob {
          name = "${request.parent}/rankIndexBlobs/snapshot"
          blobType = RankIndexBlob.BlobType.SNAPSHOT
          cmmsModelLine = MODEL_LINE
          if (request.parent == UPLOAD && deleted) {
            deleteTime = Timestamp.getDefaultInstance()
          }
        }
      }
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/up1"
    private const val PREDECESSOR_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/predecessor"
    private const val PREDECESSOR_REPLACEMENT_UPLOAD =
      "$DATA_PROVIDER/rawImpressionUploads/predecessor-replacement"
    private const val REPLACEMENT_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/up2"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private const val DONE_BLOB_URI = "gs://raw-bucket/edp/date/done"
    private const val PREDECESSOR_DONE_BLOB_URI = "gs://raw-bucket/edp/previous-date/done"
    private const val EVICTION_OPERATION_ID = "123e4567-e89b-42d3-a456-426614174000"
    private const val GENERATION = 100L
    private const val NEW_GENERATION = 50L
    private val DONE_BLOB_CREATE_TIME = Instant.parse("2026-09-01T00:00:00Z")
  }
}
