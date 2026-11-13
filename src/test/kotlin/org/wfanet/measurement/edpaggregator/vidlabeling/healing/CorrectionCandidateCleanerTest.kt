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
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.mockingDetails
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.PurgeRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.purgeRawImpressionUploadCorrectionCandidatesResponse

@RunWith(JUnit4::class)
class CorrectionCandidateCleanerTest {
  private val recordingThrottlers = VidLabelingRpcThrottlersTestHelper.recording()

  @After
  fun verifyMutationPacing() {
    val calls =
      mockingDetails(service).invocations.count {
        it.method.name == "purgeRawImpressionUploadCorrectionCandidates" &&
          it.method.parameterCount == 2
      }
    assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(calls)
    assertThat(recordingThrottlers.metadataRead.invocationCount).isEqualTo(0)
  }

  private val service =
    mockService<
      RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase
    >()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(service) }

  @Test
  fun `purge drains every full page`() = runBlocking {
    whenever(service.purgeRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(
        purgeRawImpressionUploadCorrectionCandidatesResponse { purgeCount = 2 },
        purgeRawImpressionUploadCorrectionCandidatesResponse { purgeCount = 2 },
        purgeRawImpressionUploadCorrectionCandidatesResponse { purgeCount = 1 },
      )
    val cleaner =
      GrpcCorrectionCandidateCleaner(
        RawImpressionUploadCorrectionCandidateServiceGrpcKt
          .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel),
        pageSize = 2,
        rpcThrottlers = recordingThrottlers.throttlers,
      )

    cleaner.purge("dataProviders/dp")

    val requests = argumentCaptor<PurgeRawImpressionUploadCorrectionCandidatesRequest>()
    verifyBlocking(service, times(3)) {
      purgeRawImpressionUploadCorrectionCandidates(requests.capture())
    }
    assertThat(requests.allValues.map { it.parent })
      .containsExactly("dataProviders/dp", "dataProviders/dp", "dataProviders/dp")
    assertThat(requests.allValues.map { it.pageSize }).containsExactly(2, 2, 2)
    Unit
  }

  @Test
  fun `purge rejects an invalid DataProvider name`() = runBlocking {
    val cleaner =
      GrpcCorrectionCandidateCleaner(
        RawImpressionUploadCorrectionCandidateServiceGrpcKt
          .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel),
        rpcThrottlers = recordingThrottlers.throttlers,
      )

    val error = assertFailsWith<IllegalArgumentException> { cleaner.purge("dp") }

    assertThat(error).hasMessageThat().contains("Invalid DataProvider")
    Unit
  }

  @Test
  fun `purge stops after an empty page`() =
    runBlocking<Unit> {
      whenever(service.purgeRawImpressionUploadCorrectionCandidates(any()))
        .thenReturn(purgeRawImpressionUploadCorrectionCandidatesResponse {})
      newCleaner().purge("dataProviders/dp")
      assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(1)
    }

  @Test
  fun `purge rejects response counts outside the requested page size`() =
    runBlocking<Unit> {
      for (count in listOf(-1, 3)) {
        whenever(service.purgeRawImpressionUploadCorrectionCandidates(any()))
          .thenReturn(purgeRawImpressionUploadCorrectionCandidatesResponse { purgeCount = count })
        assertFailsWith<IllegalStateException> { newCleaner().purge("dataProviders/dp") }
      }
      assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(2)
    }

  @Test
  fun `purge rejects page sizes outside the service limits`() {
    for (pageSize in listOf(0, 1001)) {
      assertFailsWith<IllegalArgumentException> { newCleaner(pageSize) }
    }
  }

  @Test
  fun `purge paces and propagates a failure on a later page`() =
    runBlocking<Unit> {
      whenever(service.purgeRawImpressionUploadCorrectionCandidates(any()))
        .thenReturn(purgeRawImpressionUploadCorrectionCandidatesResponse { purgeCount = 2 })
        .thenThrow(Status.UNAVAILABLE.asRuntimeException())
      val failure = assertFailsWith<StatusException> { newCleaner().purge("dataProviders/dp") }
      assertThat(failure.status.code).isEqualTo(Status.Code.UNAVAILABLE)
      assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(2)
    }

  @Test
  fun `cancelling a purge waiting for a mutation permit sends no request`() =
    runBlocking<Unit> {
      val entered = CompletableDeferred<Unit>()
      val release = CompletableDeferred<Unit>()
      val gate =
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T {
            entered.complete(Unit)
            release.await()
            return recordingThrottlers.metadataWrite.onReady(block)
          }
        }
      val cleaner =
        GrpcCorrectionCandidateCleaner(
          RawImpressionUploadCorrectionCandidateServiceGrpcKt
            .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel),
          recordingThrottlers.throttlers.copy(metadataWrite = gate),
        )
      val purge = async { cleaner.purge("dataProviders/dp") }
      entered.await()
      purge.cancel()
      assertFailsWith<CancellationException> { purge.await() }
      assertThat(recordingThrottlers.metadataWrite.invocationCount).isEqualTo(0)
    }

  private fun newCleaner(pageSize: Int = 2): GrpcCorrectionCandidateCleaner =
    GrpcCorrectionCandidateCleaner(
      RawImpressionUploadCorrectionCandidateServiceGrpcKt
        .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel),
      recordingThrottlers.throttlers,
      pageSize,
    )
}
