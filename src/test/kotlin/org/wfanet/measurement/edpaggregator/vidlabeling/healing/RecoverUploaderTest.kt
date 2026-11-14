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

package org.wfanet.measurement.edpaggregator.vidlabeling.healing

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.mockingDetails
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

@RunWith(JUnit4::class)
class RecoverUploaderTest {
  private val recordingThrottlers = VidLabelingRpcThrottlersTestHelper.recording()

  @After
  fun verifyRpcPacing() {
    val calls =
      listOf(uploadsService, modelLinesService, rankIndexBlobsService)
        .flatMap { mockingDetails(it).invocations }
        .filter { it.method.parameterCount == 2 }
        .filter {
          it.method.name.startsWith("get") ||
            it.method.name.startsWith("list") ||
            it.method.name.startsWith("acquire") ||
            it.method.name.startsWith("advance") ||
            it.method.name.startsWith("release") ||
            it.method.name.startsWith("mark") ||
            it.method.name.startsWith("delete") ||
            it.method.name.startsWith("batchDelete") ||
            it.method.name.startsWith("reconcile") ||
            it.method.name.startsWith("finalize")
        }
    val reads =
      calls.count { it.method.name.startsWith("get") || it.method.name.startsWith("list") }
    assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(reads)
    assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(calls.size - reads)
    assertThat(recordingThrottlers.kingdom.invocationCount).isEqualTo(0)
    assertThat(recordingThrottlers.controlPlane.invocationCount).isEqualTo(0)
  }

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
  fun `recover starts a new memoized chain when no predecessor remains`() = runBlocking {
    stubSourceUpload()
    stubModelLine(RawImpressionUploadModelLine.State.FAILED, recoveryPredecessorUpload = "")
    stubSnapshot()
    var rewriteCalled = false
    val recoverUploader = recoverUploader { _, _, _ ->
      rewriteCalled = true
      NEW_GENERATION
    }

    val result = recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))

    assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION)
    assertThat(rewriteCalled).isTrue()
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
  fun `recover uses the completed planned predecessor before a newer deferred revision`() =
    runBlocking {
      val operationName = "$DATA_PROVIDER/uploadHealingOperations/$EVICTION_OPERATION_ID"
      val deferred = rawImpressionUpload {
        name = "$DATA_PROVIDER/rawImpressionUploads/later-predecessor"
        doneBlobUri = PREDECESSOR_DONE_BLOB_URI
        doneBlobGeneration = GENERATION + 1
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime()
        replacesRawImpressionUpload = PREDECESSOR_REPLACEMENT_UPLOAD
        state = RawImpressionUpload.State.CREATED
        processingDeferred = true
        registrationComplete = true
      }
      stubSourceUpload(deferred)
      stubModelLine(
        RawImpressionUploadModelLine.State.FAILED,
        predecessorState = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
        blockedUpload = deferred.name,
      )
      stubSnapshot()
      val operation = uploadHealingOperation {
        name = operationName
        state = UploadHealingOperation.State.RECOVERING
        steps += uploadHealingStep {
          sourceRawImpressionUpload = PREDECESSOR_UPLOAD
          cmmsModelLine = MODEL_LINE
          recoveryTarget = true
          state = UploadHealingStep.State.COMPLETE
          replacementRawImpressionUpload = PREDECESSOR_REPLACEMENT_UPLOAD
        }
        steps += uploadHealingStep {
          sourceRawImpressionUpload = UPLOAD
          rawImpressionUploadModelLine = "$UPLOAD/rawImpressionUploadModelLines/rml1"
          cmmsModelLine = MODEL_LINE
          recoveryTarget = true
          recoveryAction =
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
          state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
          recoveryPredecessorRawImpressionUpload = PREDECESSOR_UPLOAD
        }
      }
      val recoverUploader =
        recoverUploader(
          getHealingOperation = { operation },
          rewriteDoneBlob = { _, _, _ -> NEW_GENERATION },
        )

      val result = recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))

      assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION)
      assertThat(deferred.processingDeferred).isTrue()
    }

  @Test
  fun `recover retains the source marker when a newer source revision is deferred`() = runBlocking {
    val deferred = rawImpressionUpload {
      name = REPLACEMENT_UPLOAD
      doneBlobUri = DONE_BLOB_URI
      doneBlobGeneration = NEW_GENERATION
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime()
      replacesRawImpressionUpload = UPLOAD
      processingDeferred = true
      state = RawImpressionUpload.State.CORRECTION_REQUIRED
    }
    stubSourceUpload(deferred)
    stubModelLine(RawImpressionUploadModelLine.State.FAILED, recoveryPredecessorUpload = "")
    stubSnapshot()
    val operation = uploadHealingOperation {
      name = "$DATA_PROVIDER/uploadHealingOperations/$EVICTION_OPERATION_ID"
      state = UploadHealingOperation.State.RECOVERING
      steps += uploadHealingStep {
        sourceRawImpressionUpload = UPLOAD
        cmmsModelLine = MODEL_LINE
        recoveryTarget = true
        state = UploadHealingStep.State.WAITING_FOR_REPLACEMENT
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
      }
    }
    val uploader =
      recoverUploader(
        getHealingOperation = { operation },
        rewriteDoneBlob = { _, _, _ -> error("A newer EDP marker must be preserved") },
      )

    assertThat(uploader.recover(UPLOAD, listOf(MODEL_LINE)).doneBlobGeneration)
      .isEqualTo(GENERATION)
  }

  @Test
  fun `recover rejects a superseded upload`() = runBlocking {
    stubSourceUpload(
      rawImpressionUpload {
        name = REPLACEMENT_UPLOAD
        state = RawImpressionUpload.State.COMPLETED
        doneBlobUri = DONE_BLOB_URI
        doneBlobGeneration = NEW_GENERATION
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime()
        replacesRawImpressionUpload = UPLOAD
        registrationComplete = true
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
  fun `recover resumes when latest revision is an incomplete recovery`() = runBlocking {
    stubSourceUpload(
      rawImpressionUpload {
        name = REPLACEMENT_UPLOAD
        state = RawImpressionUpload.State.CREATED
        doneBlobUri = DONE_BLOB_URI
        doneBlobGeneration = NEW_GENERATION
        doneBlobCreateTime = DONE_BLOB_CREATE_TIME.plusSeconds(1).toProtoTime()
        replacesRawImpressionUpload = UPLOAD
        registrationComplete = false
      }
    )
    stubModelLine(RawImpressionUploadModelLine.State.FAILED)
    stubSnapshot()
    var rewriteExpectedGeneration: Long? = null
    val recoverUploader = recoverUploader { _, expectedGeneration, _ ->
      rewriteExpectedGeneration = expectedGeneration
      NEW_GENERATION - 1
    }

    val result = recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))

    assertThat(rewriteExpectedGeneration).isEqualTo(GENERATION)
    assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION - 1)
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

      assertThat(error).hasMessageThat().contains("has not produced durable labeled output")
      assertThat(rewriteCalled).isFalse()
    }

  @Test
  fun `recover accepts a predecessor waiting for availability synchronization`() = runBlocking {
    stubSourceUpload()
    stubModelLine(
      RawImpressionUploadModelLine.State.FAILED,
      predecessorState = RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
    )
    stubSnapshot()
    var rewriteCalled = false
    val recoverUploader = recoverUploader { _, _, _ ->
      rewriteCalled = true
      NEW_GENERATION
    }

    val result = recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))

    assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION)
    assertThat(rewriteCalled).isTrue()
  }

  @Test
  fun `recover accepts exact historical AS predecessor without a completed replacement revision`():
    Unit = runBlocking {
    val fixture = historicalFixture()
    var rewritten = false
    val result =
      recoverUploader(
          getHealingOperation = { fixture },
          rewriteDoneBlob = { _, _, _ ->
            rewritten = true
            NEW_GENERATION
          },
        )
        .recover(UPLOAD, listOf(MODEL_LINE))
    assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION)
    assertThat(rewritten).isTrue()
  }

  @Test
  fun `recover reads every original and historical snapshot page before rewrite`(): Unit =
    runBlocking {
      val fixture = historicalFixture(paginated = true)
      val result =
        recoverUploader(
            getHealingOperation = { fixture },
            rewriteDoneBlob = { _, _, _ -> NEW_GENERATION },
          )
          .recover(UPLOAD, listOf(MODEL_LINE))
      assertThat(result.doneBlobGeneration).isEqualTo(NEW_GENERATION)
    }

  @Test
  fun `recover rejects historical AS when approved target is not memoized`(): Unit = runBlocking {
    val fixture = historicalFixture(memoized = false)
    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader(
            getHealingOperation = { fixture },
            rewriteDoneBlob = { _, _, _ -> error("No rewrite") },
          )
          .recover(UPLOAD, listOf(MODEL_LINE))
      }
    assertThat(error).hasMessageThat().contains("No upload revision")
  }

  @Test
  fun `recover rejects historical AS when its model row is missing`(): Unit = runBlocking {
    val fixture = historicalFixture(missingPredecessorRow = true)
    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader(
            getHealingOperation = { fixture },
            rewriteDoneBlob = { _, _, _ -> error("No rewrite") },
          )
          .recover(UPLOAD, listOf(MODEL_LINE))
      }
    assertThat(error).hasMessageThat().contains("No upload revision")
  }

  @Test
  fun `recover leaves marker unchanged when historical source lookup is unavailable`(): Unit =
    runBlocking {
      val fixture = historicalFixture(sourceLookupError = Status.UNAVAILABLE)
      val failure =
        assertFailsWith<StatusException> {
          recoverUploader(
              getHealingOperation = { fixture },
              rewriteDoneBlob = { _, _, _ -> error("No rewrite") },
            )
            .recover(UPLOAD, listOf(MODEL_LINE))
        }
      assertThat(failure.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    }

  @Test
  fun `recover propagates cancelled historical source metadata lookup`(): Unit = runBlocking {
    val fixture = historicalFixture(sourceLookupError = Status.CANCELLED)
    val failure =
      assertFailsWith<StatusException> {
        recoverUploader(
            getHealingOperation = { fixture },
            rewriteDoneBlob = { _, _, _ -> error("No rewrite") },
          )
          .recover(UPLOAD, listOf(MODEL_LINE))
      }
    assertThat(failure.status.code).isEqualTo(Status.Code.CANCELLED)
  }

  @Test
  fun `recover preserves exact completed external predecessor without historical exemption`():
    Unit = runBlocking {
    val fixture = historicalFixture(predecessorCompleted = true)
    assertThat(
        recoverUploader(
            getHealingOperation = { fixture },
            rewriteDoneBlob = { _, _, _ -> NEW_GENERATION },
          )
          .recover(UPLOAD, listOf(MODEL_LINE))
          .doneBlobGeneration
      )
      .isEqualTo(NEW_GENERATION)
  }

  @Test
  fun `recover rejects exact AS predecessor with a later unapproved snapshot basis`(): Unit =
    runBlocking {
      val fixture = historicalFixture(laterSnapshot = true)
      val failure =
        assertFailsWith<IllegalArgumentException> {
          recoverUploader(
              getHealingOperation = { fixture },
              rewriteDoneBlob = { _, _, _ -> error("No rewrite") },
            )
            .recover(UPLOAD, listOf(MODEL_LINE))
        }
      assertThat(failure).hasMessageThat().contains("No upload revision")
    }

  @Test
  fun `recover rejects exact AS predecessor owned by another healing operation`(): Unit =
    runBlocking {
      val fixture = historicalFixture(foreignPredecessor = true)
      val failure =
        assertFailsWith<IllegalArgumentException> {
          recoverUploader(
              getHealingOperation = { fixture },
              rewriteDoneBlob = { _, _, _ -> error("No rewrite") },
            )
            .recover(UPLOAD, listOf(MODEL_LINE))
        }
      assertThat(failure).hasMessageThat().contains("No upload revision")
    }

  @Test
  fun `recover historical proof filters mismatched model target and recovery action`(): Unit =
    runBlocking {
      val fixture = historicalFixture(scopeDecoys = true)
      assertThat(
          recoverUploader(
              getHealingOperation = { fixture },
              rewriteDoneBlob = { _, _, _ -> NEW_GENERATION },
            )
            .recover(UPLOAD, listOf(MODEL_LINE))
            .doneBlobGeneration
        )
        .isEqualTo(NEW_GENERATION)
    }

  private suspend fun historicalFixture(
    paginated: Boolean = false,
    memoized: Boolean = true,
    missingPredecessorRow: Boolean = false,
    sourceLookupError: Status? = null,
    predecessorCompleted: Boolean = false,
    laterSnapshot: Boolean = false,
    foreignPredecessor: Boolean = false,
    scopeDecoys: Boolean = false,
  ): UploadHealingOperation {
    val source = rawImpressionUpload {
      name = UPLOAD
      state = RawImpressionUpload.State.FAILED
      registrationComplete = true
      doneBlobUri = DONE_BLOB_URI
      doneBlobGeneration = GENERATION
      createTime = DONE_BLOB_CREATE_TIME.minusSeconds(10).toProtoTime()
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.toProtoTime()
    }
    val predecessor = rawImpressionUpload {
      name = PREDECESSOR_UPLOAD
      if (foreignPredecessor)
        uploadHealingOperation = "$DATA_PROVIDER/uploadHealingOperations/foreign"
      state =
        if (predecessorCompleted) RawImpressionUpload.State.COMPLETED
        else RawImpressionUpload.State.ACTIVE
      registrationComplete = true
      doneBlobUri = PREDECESSOR_DONE_BLOB_URI
      doneBlobGeneration = GENERATION - 2
      createTime = DONE_BLOB_CREATE_TIME.minusSeconds(20).toProtoTime()
      doneBlobCreateTime = DONE_BLOB_CREATE_TIME.minusSeconds(2).toProtoTime()
    }
    var sourceGets = 0
    whenever(uploadsService.getRawImpressionUpload(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadRequest
        >(
          0
        )
      if (request.name == UPLOAD) {
        sourceGets++
        if (sourceGets == 2 && sourceLookupError != null)
          throw sourceLookupError.asRuntimeException()
        source
      } else predecessor
    }
    whenever(uploadsService.listRawImpressionUploads(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequest
        >(
          0
        )
      listRawImpressionUploadsResponse {
        rawImpressionUploads +=
          if (request.filter.doneBlobUri == DONE_BLOB_URI) source else predecessor
      }
    }
    stubModelLine(
      RawImpressionUploadModelLine.State.FAILED,
      predecessorState =
        if (predecessorCompleted) RawImpressionUploadModelLine.State.COMPLETED
        else RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
    )
    if (missingPredecessorRow)
      whenever(modelLinesService.listRawImpressionUploadModelLines(any())).thenAnswer { invocation
        ->
        val request =
          invocation.getArgument<
            org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
          >(
            0
          )
        listRawImpressionUploadModelLinesResponse {
          if (request.parent == UPLOAD)
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = "$UPLOAD/rawImpressionUploadModelLines/rml1"
              cmmsModelLine = MODEL_LINE
              state = RawImpressionUploadModelLine.State.FAILED
              failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
              recoveryAction =
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
              evictionOperationId = EVICTION_OPERATION_ID
              recoveryPredecessorRawImpressionUpload = PREDECESSOR_UPLOAD
            }
        }
      }
    whenever(rankIndexBlobsService.listRankIndexBlobs(any())).thenAnswer { invocation ->
      val request =
        invocation.getArgument<
          org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequest
        >(
          0
        )
      listRankIndexBlobsResponse {
        rankIndexBlobs += rankIndexBlob {
          name = "${request.parent}/rankIndexBlobs/${request.pageToken.ifEmpty { "first" }}"
          cmmsModelLine = MODEL_LINE
          blobType = RankIndexBlob.BlobType.SNAPSHOT
          poolOffset = if (request.pageToken.isEmpty()) 0L else 1L
          createTime =
            DONE_BLOB_CREATE_TIME.plusSeconds(
                if (request.parent == UPLOAD) 10 else if (laterSnapshot) 15 else -5
              )
              .toProtoTime()
          if (request.parent == UPLOAD)
            deleteTime = DONE_BLOB_CREATE_TIME.plusSeconds(20).toProtoTime()
        }
        if (paginated && request.pageSize != 1 && request.pageToken.isEmpty())
          nextPageToken = "second"
      }
    }
    return uploadHealingOperation {
      name = "$DATA_PROVIDER/uploadHealingOperations/$EVICTION_OPERATION_ID"
      state = UploadHealingOperation.State.RECOVERING
      steps += uploadHealingStep {
        sourceRawImpressionUpload = "$DATA_PROVIDER/rawImpressionUploads/other"
        rawImpressionUploadModelLine = "other-model-row"
        cmmsModelLine = MODEL_LINE_2
        recoveryTarget = true
        this.memoized = true
      }
      if (scopeDecoys) {
        for (kind in listOf("model", "target", "action")) steps += uploadHealingStep {
          sourceRawImpressionUpload = "$DATA_PROVIDER/rawImpressionUploads/decoy-$kind"
          rawImpressionUploadModelLine = "$UPLOAD/rawImpressionUploadModelLines/rml1"
          cmmsModelLine = if (kind == "model") MODEL_LINE_2 else MODEL_LINE
          recoveryTarget = kind != "target"
          this.memoized = true
          recoveryAction =
            if (kind == "action")
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
            else RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        }
      }
      steps += uploadHealingStep {
        sourceRawImpressionUpload = UPLOAD
        rawImpressionUploadModelLine = "$UPLOAD/rawImpressionUploadModelLines/rml1"
        cmmsModelLine = MODEL_LINE
        recoveryTarget = true
        this.memoized = memoized
        recoveryAction =
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
      }
    }
  }

  private fun recoverUploader(
    getHealingOperation: suspend (String) -> UploadHealingOperation? = { null },
    rewriteDoneBlob: suspend (String, Long, Map<String, String>) -> Long,
  ): RecoverUploader {
    val channel = grpcTestServerRule.channel
    return RecoverUploader(
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel),
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      ),
      RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub(channel),
      getHealingOperation = getHealingOperation,
      rewriteDoneBlob = rewriteDoneBlob,
      rpcThrottlers = recordingThrottlers.throttlers,
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
      uploadHealingOperation = "$DATA_PROVIDER/uploadHealingOperations/$EVICTION_OPERATION_ID"
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
          rawImpressionUploads +=
            additionalUploads.filter { it.doneBlobUri == PREDECESSOR_DONE_BLOB_URI }
        } else {
          rawImpressionUploads += source
          for (upload in additionalUploads.filter { it.doneBlobUri != PREDECESSOR_DONE_BLOB_URI }) {
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
    recoveryPredecessorUpload: String = PREDECESSOR_UPLOAD,
    blockedUpload: String = "",
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
          this.state =
            when (request.parent) {
              UPLOAD -> state
              blockedUpload -> RawImpressionUploadModelLine.State.CREATED
              else -> predecessorState
            }
          if (request.parent == UPLOAD) {
            failureReason = RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
            this.recoveryAction = recoveryAction
            evictionOperationId = EVICTION_OPERATION_ID
            if (recoveryPredecessorUpload.isNotEmpty()) {
              recoveryPredecessorRawImpressionUpload = recoveryPredecessorUpload
            }
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
