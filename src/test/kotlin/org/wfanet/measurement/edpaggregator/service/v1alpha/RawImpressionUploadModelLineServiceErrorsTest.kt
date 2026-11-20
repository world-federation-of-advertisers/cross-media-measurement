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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.service.internal.PoolAssignmentNotReadyException
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.internal.edpaggregator.MarkRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt

class RawImpressionUploadModelLineServiceErrorsTest {
  private var internalFailure = Status.FAILED_PRECONDITION.asRuntimeException()
  private val backend =
    object :
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase() {
      override suspend fun markRawImpressionUploadModelLineLabeling(
        request: MarkRawImpressionUploadModelLineLabelingRequest
      ): RawImpressionUploadModelLine = throw internalFailure
    }
  @get:Rule val grpcServer = GrpcTestServerRule { addService(backend) }

  private suspend fun callLabeling(): StatusRuntimeException {
    val service =
      RawImpressionUploadModelLineService(
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
          grpcServer.channel
        )
      )
    return assertFailsWith {
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          name = RawImpressionUploadModelLineKey("dp", "upload", "model").toName()
          etag = "etag"
          requestId = UUID.randomUUID().toString()
        }
      )
    }
  }

  @Test
  fun `labeling maps phase zero evidence to stable public ErrorInfo`() = runBlocking {
    val internal =
      PoolAssignmentNotReadyException("dp", "upload", CMMS_MODEL_LINE)
        .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    val returned =
      listOf("private-diagnostic-a", "private-diagnostic-b").map { description ->
        internalFailure =
          internal.status.withDescription(description).asRuntimeException(internal.trailers)
        val error = callLabeling()
        assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
        assertThat(error.errorInfo!!.domain).isEqualTo(Errors.DOMAIN)
        assertThat(error.errorInfo!!.reason).isEqualTo(Errors.Reason.POOL_ASSIGNMENT_NOT_READY.name)
        assertThat(error.errorInfo!!.metadataMap[Errors.Metadata.RAW_IMPRESSION_UPLOAD.key])
          .isEqualTo(RawImpressionUploadKey("dp", "upload").toName())
        assertThat(error.errorInfo!!.metadataMap[Errors.Metadata.CMMS_MODEL_LINE.key])
          .isEqualTo(CMMS_MODEL_LINE)
        assertThat(error.status.description).doesNotContain(description)
        error
      }
    assertThat(returned[0].status.description).isEqualTo(returned[1].status.description)
    assertThat(returned[0].errorInfo).isEqualTo(returned[1].errorInfo)
  }

  @Test
  fun `labeling does not echo an unclassified internal precondition description`() = runBlocking {
    internalFailure =
      Status.FAILED_PRECONDITION.withDescription("private-unclassified-description")
        .asRuntimeException()
    val error = callLabeling()
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(error.errorInfo!!.domain).isEqualTo(Errors.DOMAIN)
    assertThat(error.errorInfo!!.reason).isEqualTo(Errors.Reason.PRECONDITION_FAILED.name)
    assertThat(error.status.description).doesNotContain("private-unclassified-description")
  }

  companion object {
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
  }
}
