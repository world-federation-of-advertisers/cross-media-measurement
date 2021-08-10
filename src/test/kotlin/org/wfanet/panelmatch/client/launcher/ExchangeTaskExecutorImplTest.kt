// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.launcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.fold
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.launcher.testing.FakeTimeout
import org.wfanet.panelmatch.client.launcher.testing.buildStep
import org.wfanet.panelmatch.client.storage.InMemoryStorage
import org.wfanet.panelmatch.client.storage.toByteString
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ExchangeTaskExecutorImplTest {
  private val apiClient: ApiClient = mock()
  private val privateStorage = InMemoryStorage(keyPrefix = "private")
  private val sharedStorage = InMemoryStorage(keyPrefix = "shared")

  private val timeout = FakeTimeout()

  private val exchangeTask =
    object : ExchangeTask {
      override suspend fun execute(
        input: Map<String, StorageClient.Blob>
      ): Map<String, Flow<ByteString>> {
        return input.mapKeys { "Out:${it.key}" }.mapValues {
          val valString: String =
            it.value
              .read(1024)
              .fold(ByteString.EMPTY, { agg, chunk -> agg.concat(chunk) })
              .toStringUtf8()
          ByteString.copyFromUtf8("Out:$valString").asBufferedFlow(1024)
        }
      }
    }

  private val exchangeTaskExecutor =
    ExchangeTaskExecutorImpl(apiClient, timeout, sharedStorage, privateStorage) { exchangeTask }

  @Test
  fun `reads inputs and writes outputs`() = runBlockingTest {
    val blob1 = ByteString.copyFromUtf8("blob1")
    val blob2 = ByteString.copyFromUtf8("blob2")

    privateStorage.batchWrite(
      outputLabels = mapOf("a" to "b"),
      data = mapOf("a" to blob1.asBufferedFlow(1024))
    )
    sharedStorage.batchWrite(
      outputLabels = mapOf("c" to "d"),
      data = mapOf("c" to blob2.asBufferedFlow(1024))
    )

    exchangeTaskExecutor.execute(
      ExchangeStepAttemptKey("w", "x", "y", "z"),
      buildStep(
        ENCRYPT_STEP,
        privateInputLabels = mapOf("a" to "b"),
        sharedInputLabels = mapOf("c" to "d"),
        privateOutputLabels = mapOf("Out:c" to "e"),
        sharedOutputLabels = mapOf("Out:a" to "f")
      )
    )

    assertThat(
        privateStorage.batchRead(mapOf("Out:c" to "e")).mapValues { it.value.toByteString() }
      )
      .containsExactly("Out:c", ByteString.copyFromUtf8("Out:blob2"))

    assertThat(sharedStorage.batchRead(mapOf("Out:a" to "f")).mapValues { it.value.toByteString() })
      .containsExactly("Out:a", ByteString.copyFromUtf8("Out:blob1"))
  }

  @Test
  fun timeout() = runBlockingTest {
    timeout.expired = true

    privateStorage.batchWrite(
      outputLabels = mapOf("a" to "b"),
      data = mapOf("a" to emptyFlow<ByteString>())
    )

    assertFailsWith<CancellationException> {
      exchangeTaskExecutor.execute(
        ExchangeStepAttemptKey("w", "x", "y", "z"),
        buildStep(
          ENCRYPT_STEP,
          privateInputLabels = mapOf("a" to "b"),
          sharedOutputLabels = mapOf("Out:a" to "c")
        )
      )
    }

    assertFails { sharedStorage.batchRead(mapOf("Out:a" to "c")) }
  }
}
