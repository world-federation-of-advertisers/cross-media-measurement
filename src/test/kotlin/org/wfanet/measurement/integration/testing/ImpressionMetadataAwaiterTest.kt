// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.testing

import com.google.common.truth.Truth.assertThat
import io.grpc.Context
import io.grpc.Status
import io.grpc.StatusException
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataResponse

@OptIn(ExperimentalCoroutinesApi::class)
class ImpressionMetadataAwaiterTest {
  private val requests = mutableListOf<ListImpressionMetadataRequest>()
  private val deadlines = mutableListOf<Long>()
  private var read: (ListImpressionMetadataRequest) -> ListImpressionMetadataResponse = {
    listImpressionMetadataResponse { impressionMetadata += CURRENT }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(
      object : ImpressionMetadataServiceCoroutineImplBase(Dispatchers.Unconfined) {
        override suspend fun listImpressionMetadata(
          request: ListImpressionMetadataRequest
        ): ListImpressionMetadataResponse {
          requests += request
          deadlines += checkNotNull(Context.current().deadline).timeRemaining(TimeUnit.MILLISECONDS)
          return read(request)
        }
      }
    )
  }

  @Test
  fun `waits for a rewritten fixture before allowing measurements`() = runTest {
    var measurementsStarted = false
    read = {
      assertThat(measurementsStarted).isFalse()
      listImpressionMetadataResponse {
        impressionMetadata +=
          if (requests.size == 1) CURRENT.copy { metadataBlobGeneration = 19 } else CURRENT
      }
    }

    awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT)))
    measurementsStarted = true

    assertThat(measurementsStarted).isTrue()
    assertThat(requests).hasSize(2)
    assertThat(testScheduler.currentTime).isEqualTo(1000L)
    assertThat(requests.first().filter.blobUrisList).containsExactly(CURRENT.blobUri)
    assertThat(requests.first().showDeleted).isTrue()
    assertThat(deadlines.all { it in 1L..30_000L }).isTrue()
  }

  @Test
  fun `partial synchronization across providers cannot start measurements`() = runTest {
    val other = CURRENT.copy { blobUri = "gs://fixtures/other/metadata.binpb" }
    read = { request ->
      listImpressionMetadataResponse {
        if (request.parent == PARENT) impressionMetadata += CURRENT
        else if (requests.size > 2) impressionMetadata += other
      }
    }

    awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT), OTHER_PARENT to listOf(other)))

    assertThat(requests).hasSize(4)
    assertThat(testScheduler.currentTime).isEqualTo(1000L)
  }

  @Test
  fun `checks every page before declaring all fixtures ready`() = runTest {
    val other = CURRENT.copy { blobUri = "gs://fixtures/second/metadata.binpb" }
    read = { request ->
      listImpressionMetadataResponse {
        if (request.pageToken.isEmpty()) {
          impressionMetadata += CURRENT
          nextPageToken = "next"
        } else impressionMetadata += other
      }
    }

    awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT, other)))

    assertThat(requests.map { it.pageToken }).containsExactly("", "next").inOrder()
  }

  @Test
  fun `stale generations deleted records and wrong model lines time out`() = runTest {
    val unusable =
      listOf(
        CURRENT.copy { metadataBlobGeneration = 0 },
        CURRENT.copy { metadataBlobGeneration = 19 },
        CURRENT.copy { impressionsBlobGeneration = 29 },
        CURRENT.copy { outputDoneBlobGeneration = 39 },
        CURRENT.copy { state = ImpressionMetadata.State.DELETED },
        CURRENT.copy { modelLine = "modelProviders/mp/modelSuites/ms/modelLines/other" },
      )
    for (metadata in unusable) {
      read = { listImpressionMetadataResponse { impressionMetadata += metadata } }
      val failure =
        assertFailsWith<IllegalStateException> {
          awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT)))
        }
      assertThat(failure).hasMessageThat().contains(CURRENT.blobUri)
      assertThat(failure).hasMessageThat().contains("expected metadata=20, impressions=30, done=40")
    }
  }

  @Test
  fun `missing fixtures time out without allowing measurements`() = runTest {
    read = { listImpressionMetadataResponse {} }
    var measurementsStarted = false

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT)))
        measurementsStarted = true
      }

    assertThat(measurementsStarted).isFalse()
    assertThat(failure).hasMessageThat().contains("observed missing")
  }

  @Test
  fun `retries transient reads then accepts current generations`() = runTest {
    read = {
      if (requests.size == 1) throw Status.UNAVAILABLE.asException()
      listImpressionMetadataResponse { impressionMetadata += CURRENT }
    }

    awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT)))

    assertThat(requests).hasSize(2)
  }

  @Test
  fun `persistent transient errors retain the cause at the timeout`() = runTest {
    read = { throw Status.INTERNAL.withDescription("interrupted").asException() }

    val failure =
      assertFailsWith<IllegalStateException> {
        awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT)))
      }

    assertThat((failure.cause as StatusException).status.code).isEqualTo(Status.Code.INTERNAL)
  }

  @Test
  fun `permission failures stop immediately`() = runTest {
    read = { throw Status.PERMISSION_DENIED.asException() }

    assertFailsWith<StatusException> { awaiter().awaitCompletion(mapOf(PARENT to listOf(CURRENT))) }
    assertThat(requests).hasSize(1)
  }

  private fun awaiter() =
    ImpressionMetadataAwaiter(
      ImpressionMetadataServiceCoroutineStub(grpcTestServerRule.channel),
      pollInterval = 1.seconds,
      timeout = 3.seconds,
      rpcTimeout = 30.seconds,
    )

  companion object {
    private const val PARENT = "dataProviders/edp7"
    private const val OTHER_PARENT = "dataProviders/meta"
    private val CURRENT = impressionMetadata {
      blobUri = "gs://fixtures/first/metadata.binpb"
      modelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml"
      state = ImpressionMetadata.State.ACTIVE
      metadataBlobGeneration = 20
      impressionsBlobGeneration = 30
      outputDoneBlobGeneration = 40
    }
  }
}
