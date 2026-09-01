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
          mapOf(WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to MODEL_LINE),
        )
      )
  }

  @Test
  fun `recover rejects a superseded upload`() = runBlocking {
    stubSourceUpload(
      rawImpressionUpload {
        name = REPLACEMENT_UPLOAD
        doneBlobUri = DONE_BLOB_URI
        doneBlobGeneration = NEW_GENERATION
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
  fun `recover rejects a non-memoized model line`() = runBlocking {
    stubSourceUpload()
    stubModelLine(RawImpressionUploadModelLine.State.FAILED)
    whenever(rankIndexBlobsService.listRankIndexBlobs(any()))
      .thenReturn(listRankIndexBlobsResponse {})
    val recoverUploader = recoverUploader { _, _, _ -> NEW_GENERATION }

    val error =
      assertFailsWith<IllegalArgumentException> {
        recoverUploader.recover(UPLOAD, listOf(MODEL_LINE))
      }

    assertThat(error).hasMessageThat().contains("only accepts memoized model lines")
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
    }
    whenever(uploadsService.getRawImpressionUpload(any())).thenReturn(source)
    whenever(uploadsService.listRawImpressionUploads(any()))
      .thenReturn(
        listRawImpressionUploadsResponse {
          rawImpressionUploads += source
          for (upload in additionalUploads) {
            rawImpressionUploads += upload
          }
        }
      )
  }

  private suspend fun stubModelLine(state: RawImpressionUploadModelLine.State) {
    whenever(modelLinesService.listRawImpressionUploadModelLines(any()))
      .thenReturn(
        listRawImpressionUploadModelLinesResponse {
          rawImpressionUploadModelLines += rawImpressionUploadModelLine {
            name = "$UPLOAD/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE
            this.state = state
          }
        }
      )
  }

  private suspend fun stubSnapshot() {
    whenever(rankIndexBlobsService.listRankIndexBlobs(any()))
      .thenReturn(
        listRankIndexBlobsResponse {
          rankIndexBlobs += rankIndexBlob {
            name = "$UPLOAD/rankIndexBlobs/snapshot"
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = MODEL_LINE
          }
        }
      )
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/up1"
    private const val REPLACEMENT_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/up2"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val DONE_BLOB_URI = "gs://raw-bucket/edp/date/done"
    private const val GENERATION = 100L
    private const val NEW_GENERATION = 101L
  }
}
