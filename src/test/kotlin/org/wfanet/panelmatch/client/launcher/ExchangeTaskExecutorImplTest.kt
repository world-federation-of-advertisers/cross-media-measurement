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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.launcher.testing.FakeTimeout
import org.wfanet.panelmatch.client.launcher.testing.buildStep
import org.wfanet.panelmatch.client.storage.InMemoryStorage
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ExchangeTaskExecutorImplTest {
  private val apiClient: ApiClient = mock()
  private val privateStorage = InMemoryStorage(keyPrefix = "private")
  private val sharedStorage = InMemoryStorage(keyPrefix = "shared")

  private val timeout = FakeTimeout()

  private val exchangeTask =
    object : ExchangeTask {
      override suspend fun execute(input: Map<String, ByteString>): Map<String, ByteString> {
        val result =
          input.mapKeys { "Out:${it.key}" }.mapValues {
            ByteString.copyFromUtf8("Out:${it.value.toStringUtf8()}")
          }
        return result
      }
    }

  private val exchangeTaskExecutor =
    ExchangeTaskExecutorImpl(apiClient, timeout, sharedStorage, privateStorage) { exchangeTask }

  @Test
  fun `reads inputs and writes outputs`() = runBlockingTest {
    val blob1 = ByteString.copyFromUtf8("blob1")
    val blob2 = ByteString.copyFromUtf8("blob2")

    privateStorage.batchWrite(outputLabels = mapOf("a" to "b"), data = mapOf("a" to blob1))
    sharedStorage.batchWrite(outputLabels = mapOf("c" to "d"), data = mapOf("c" to blob2))

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

    assertThat(privateStorage.batchRead(mapOf("Out:c" to "e")))
      .containsExactly("Out:c", ByteString.copyFromUtf8("Out:blob2"))

    assertThat(sharedStorage.batchRead(mapOf("Out:a" to "f")))
      .containsExactly("Out:a", ByteString.copyFromUtf8("Out:blob1"))
  }

  @Test
  fun timeout() = runBlockingTest {
    timeout.expired = true

    privateStorage.batchWrite(
      outputLabels = mapOf("a" to "b"),
      data = mapOf("a" to ByteString.EMPTY)
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
