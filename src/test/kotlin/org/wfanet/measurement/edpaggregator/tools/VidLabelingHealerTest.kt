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
import org.mockito.kotlin.argThat
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

@RunWith(JUnit4::class)
class VidLabelingHealerTest {
  private val modelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(modelLineService) }

  private val healer: VidLabelingHealer by lazy {
    VidLabelingHealer(
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        grpcTestServerRule.channel
      )
    )
  }

  @Test
  fun `markFailed marks the matching model line FAILED with the reason and etag`() {
    val existing = rawImpressionUploadModelLine {
      name = MODEL_LINE_NAME
      cmmsModelLine = MODEL_LINE
      state = RawImpressionUploadModelLine.State.RANKING
      etag = ETAG
    }
    val failed =
      existing.copy {
        state = RawImpressionUploadModelLine.State.FAILED
        errorMessage = REASON
      }

    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse { rawImpressionUploadModelLines += existing }
        )
      whenever(modelLineService.markRawImpressionUploadModelLineFailed(any())).thenReturn(failed)
      healer.markFailed(DATA_PROVIDER, UPLOAD_ID, MODEL_LINE, REASON)
    }

    assertThat(result.state).isEqualTo(RawImpressionUploadModelLine.State.FAILED)
    verifyBlocking(modelLineService) {
      markRawImpressionUploadModelLineFailed(
        argThat { name == MODEL_LINE_NAME && etag == ETAG && errorMessage == REASON }
      )
    }
  }

  @Test
  fun `markFailed is a no-op when the model line is already FAILED`() {
    val existing = rawImpressionUploadModelLine {
      name = MODEL_LINE_NAME
      cmmsModelLine = MODEL_LINE
      state = RawImpressionUploadModelLine.State.FAILED
      etag = ETAG
    }

    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse { rawImpressionUploadModelLines += existing }
        )
      healer.markFailed(DATA_PROVIDER, UPLOAD_ID, MODEL_LINE, REASON)
    }

    assertThat(result.state).isEqualTo(RawImpressionUploadModelLine.State.FAILED)
    verifyBlocking(modelLineService, never()) { markRawImpressionUploadModelLineFailed(any()) }
  }

  @Test
  fun `markFailed throws when no matching model line exists`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        runBlocking {
          whenever(modelLineService.listRawImpressionUploadModelLines(any()))
            .thenReturn(listRawImpressionUploadModelLinesResponse {})
          healer.markFailed(DATA_PROVIDER, UPLOAD_ID, MODEL_LINE, REASON)
        }
      }
    assertThat(error).hasMessageThat().contains(MODEL_LINE)
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val UPLOAD_ID = "upload1"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val MODEL_LINE_NAME =
      "$DATA_PROVIDER/rawImpressionUploads/$UPLOAD_ID/rawImpressionUploadModelLines/rml1"
    private const val ETAG = "etag-1"
    private const val REASON = "hung ranker; forcing failure"
  }
}
