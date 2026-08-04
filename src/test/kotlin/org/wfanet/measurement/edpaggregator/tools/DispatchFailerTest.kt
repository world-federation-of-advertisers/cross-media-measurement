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
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

@RunWith(JUnit4::class)
class DispatchFailerTest {
  private val modelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(modelLineService) }

  private val failer: DispatchFailer by lazy {
    DispatchFailer(
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        grpcTestServerRule.channel
      )
    )
  }

  private fun modelLine(id: String, lineState: RawImpressionUploadModelLine.State) =
    rawImpressionUploadModelLine {
      name = "$UPLOAD_NAME/rawImpressionUploadModelLines/$id"
      state = lineState
      etag = "etag-$id"
    }

  @Test
  fun `failUpload fails only the non-terminal model lines`() {
    val failed = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              modelLine("rml1", RawImpressionUploadModelLine.State.RANKING)
            rawImpressionUploadModelLines +=
              modelLine("rml2", RawImpressionUploadModelLine.State.COMPLETED)
            rawImpressionUploadModelLines +=
              modelLine("rml3", RawImpressionUploadModelLine.State.FAILED)
          }
        )
      whenever(modelLineService.markRawImpressionUploadModelLineFailed(any())).thenAnswer {
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      }
      failer.failUpload(UPLOAD_NAME, REASON)
    }

    assertThat(failed).containsExactly("$UPLOAD_NAME/rawImpressionUploadModelLines/rml1")
    verifyBlocking(modelLineService) {
      markRawImpressionUploadModelLineFailed(
        argThat {
          name == "$UPLOAD_NAME/rawImpressionUploadModelLines/rml1" && errorMessage == REASON
        }
      )
    }
  }

  @Test
  fun `failUpload marks nothing when all model lines are terminal`() {
    val failed = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              modelLine("rml1", RawImpressionUploadModelLine.State.COMPLETED)
            rawImpressionUploadModelLines +=
              modelLine("rml2", RawImpressionUploadModelLine.State.FAILED)
          }
        )
      failer.failUpload(UPLOAD_NAME, REASON)
    }

    assertThat(failed).isEmpty()
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  @Test
  fun `failUpload fails every non-terminal state in a mixed cascade`() {
    val failed = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines +=
              modelLine("c", RawImpressionUploadModelLine.State.CREATED)
            rawImpressionUploadModelLines +=
              modelLine("pa", RawImpressionUploadModelLine.State.POOL_ASSIGNING)
            rawImpressionUploadModelLines +=
              modelLine("r", RawImpressionUploadModelLine.State.RANKING)
            rawImpressionUploadModelLines +=
              modelLine("l", RawImpressionUploadModelLine.State.LABELING)
            rawImpressionUploadModelLines +=
              modelLine("done", RawImpressionUploadModelLine.State.COMPLETED)
            rawImpressionUploadModelLines +=
              modelLine("f", RawImpressionUploadModelLine.State.FAILED)
          }
        )
      whenever(modelLineService.markRawImpressionUploadModelLineFailed(any())).thenAnswer {
        rawImpressionUploadModelLine { state = RawImpressionUploadModelLine.State.FAILED }
      }
      failer.failUpload(UPLOAD_NAME, REASON)
    }

    assertThat(failed)
      .containsExactly(
        "$UPLOAD_NAME/rawImpressionUploadModelLines/c",
        "$UPLOAD_NAME/rawImpressionUploadModelLines/pa",
        "$UPLOAD_NAME/rawImpressionUploadModelLines/r",
        "$UPLOAD_NAME/rawImpressionUploadModelLines/l",
      )
  }

  @Test
  fun `failUpload marks nothing when the upload has no model lines`() {
    val failed = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(listRawImpressionUploadModelLinesResponse {})
      failer.failUpload(UPLOAD_NAME, REASON)
    }

    assertThat(failed).isEmpty()
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  @Test
  fun `failUpload propagates NOT_FOUND when the upload does not exist`() {
    assertFailsWith<StatusException> {
      runBlocking {
        whenever(modelLineService.listRawImpressionUploadModelLines(any())).thenAnswer {
          throw Status.NOT_FOUND.asRuntimeException()
        }
        failer.failUpload(UPLOAD_NAME, REASON)
      }
    }
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  companion object {
    private const val UPLOAD_NAME = "dataProviders/dp1/rawImpressionUploads/up1"
    private const val REASON = "hung ranker; forcing failure"
  }
}
