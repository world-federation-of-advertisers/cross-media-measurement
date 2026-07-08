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
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine

@RunWith(JUnit4::class)
class ModelLineBackfillerTest {
  private val modelLineService:
    RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase =
    mockService()
  private val uploadService:
    RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(modelLineService)
    addService(uploadService)
  }

  private val backfiller: ModelLineBackfiller by lazy {
    val channel = grpcTestServerRule.channel
    ModelLineBackfiller(
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        channel
      ),
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(channel),
    )
  }

  @Test
  fun `backfill creates the model line and reactivates the upload`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(listRawImpressionUploadModelLinesResponse {})
      whenever(modelLineService.createRawImpressionUploadModelLine(any()))
        .thenReturn(
          rawImpressionUploadModelLine {
            name = "$UPLOAD_NAME/rawImpressionUploadModelLines/rml1"
            cmmsModelLine = MODEL_LINE
          }
        )
      whenever(uploadService.markRawImpressionUploadActive(any()))
        .thenReturn(rawImpressionUpload { name = UPLOAD_NAME })

      backfiller.backfill(MODEL_LINE, listOf(UPLOAD_NAME))
    }

    assertThat(result.createdModelLines).hasSize(1)
    assertThat(result.reactivatedUploads).containsExactly(UPLOAD_NAME)
    verifyBlocking(modelLineService) {
      createRawImpressionUploadModelLine(
        argThat {
          parent == UPLOAD_NAME && rawImpressionUploadModelLine.cmmsModelLine == MODEL_LINE
        }
      )
    }
    verifyBlocking(uploadService) { markRawImpressionUploadActive(argThat { name == UPLOAD_NAME }) }
  }

  @Test
  fun `backfill skips create when the model line already exists`() {
    val result = runBlocking {
      whenever(modelLineService.listRawImpressionUploadModelLines(any()))
        .thenReturn(
          listRawImpressionUploadModelLinesResponse {
            rawImpressionUploadModelLines += rawImpressionUploadModelLine {
              name = "$UPLOAD_NAME/rawImpressionUploadModelLines/rml1"
              cmmsModelLine = MODEL_LINE
            }
          }
        )
      whenever(uploadService.markRawImpressionUploadActive(any()))
        .thenReturn(rawImpressionUpload { name = UPLOAD_NAME })

      backfiller.backfill(MODEL_LINE, listOf(UPLOAD_NAME))
    }

    assertThat(result.createdModelLines).isEmpty()
    assertThat(result.reactivatedUploads).containsExactly(UPLOAD_NAME)
    verifyBlocking(modelLineService, never()) { createRawImpressionUploadModelLine(any()) }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp1"
    private const val UPLOAD_ID = "upload1"
    private const val UPLOAD_NAME = "$DATA_PROVIDER/rawImpressionUploads/$UPLOAD_ID"
    private const val MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
  }
}
