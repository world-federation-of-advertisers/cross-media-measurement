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
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.edpaggregator.PurgeExpiredRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidateServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.purgeExpiredRawImpressionUploadCorrectionCandidatesResponse

@RunWith(JUnit4::class)
class CorrectionCandidateCleanerTest {
  private val service =
    mockService<
      RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineImplBase
    >()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(service) }

  @Test
  fun `purge drains every full page`() = runBlocking {
    whenever(service.purgeExpiredRawImpressionUploadCorrectionCandidates(any()))
      .thenReturn(
        purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 2 },
        purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 2 },
        purgeExpiredRawImpressionUploadCorrectionCandidatesResponse { purgedCount = 1 },
      )
    val cleaner =
      GrpcCorrectionCandidateCleaner(
        RawImpressionUploadCorrectionCandidateServiceGrpcKt
          .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel),
        pageSize = 2,
      )

    cleaner.purge("dataProviders/dp")

    val requests = argumentCaptor<PurgeExpiredRawImpressionUploadCorrectionCandidatesRequest>()
    verifyBlocking(service, times(3)) {
      purgeExpiredRawImpressionUploadCorrectionCandidates(requests.capture())
    }
    assertThat(requests.allValues.map { it.dataProviderResourceId })
      .containsExactly("dp", "dp", "dp")
    assertThat(requests.allValues.map { it.pageSize }).containsExactly(2, 2, 2)
    Unit
  }

  @Test
  fun `purge rejects an invalid DataProvider name`() = runBlocking {
    val cleaner =
      GrpcCorrectionCandidateCleaner(
        RawImpressionUploadCorrectionCandidateServiceGrpcKt
          .RawImpressionUploadCorrectionCandidateServiceCoroutineStub(grpcTestServerRule.channel)
      )

    val error = assertFailsWith<IllegalArgumentException> { cleaner.purge("dp") }

    assertThat(error).hasMessageThat().contains("Invalid DataProvider")
    Unit
  }
}
