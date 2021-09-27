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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.encryptStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.flatten
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.launcher.testing.FakeTimeout
import org.wfanet.panelmatch.client.launcher.testing.buildExchangeStep
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.client.storage.testing.makeTestVerifiedStorageClient
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class ExchangeTaskExecutorTest {
  private val apiClient: ApiClient = mock()
  private val storage = makeTestVerifiedStorageClient()

  private val timeout = FakeTimeout()

  private val exchangeTask =
    object : ExchangeTask {
      override suspend fun execute(
        input: Map<String, VerifiedStorageClient.VerifiedBlob>
      ): Map<String, Flow<ByteString>> {
        return input.mapKeys { "Out:${it.key}" }.mapValues {
          val valString: String = it.value.read(1024).flatten().toStringUtf8()
          "Out:$valString".toByteString().asBufferedFlow(1024)
        }
      }
    }

  private val exchangeTaskExecutor =
    ExchangeTaskExecutor(apiClient, timeout, storage) { exchangeTask }

  @Test
  fun `reads inputs and writes outputs`() = runBlockingTest {
    val blob = "some-blob".toByteString()

    storage.verifiedBatchWrite(
      outputLabels = mapOf("a" to "b"),
      data = mapOf("a" to blob.asBufferedFlow(1024))
    )

    exchangeTaskExecutor.execute(
      ExchangeStepAttemptKey("w", "x", "y", "z"),
      buildExchangeStep(
        name = "some-name",
        dataProviderName = "some-edp",
        modelProviderName = "some-mp",
        testedStep =
          step {
            encryptStep = encryptStep {}
            inputLabels["a"] = "b"
            outputLabels["Out:a"] = "c"
          }
      )
    )

    val readResults = storage.verifiedBatchRead(mapOf("result" to "c"))
    assertThat(readResults.mapValues { it.value.toByteString() })
      .containsExactly("result", "Out:some-blob".toByteString())
  }

  @Test
  fun timeout() = runBlockingTest {
    timeout.expired = true

    storage.verifiedBatchWrite(outputLabels = mapOf("a" to "b"), data = mapOf("a" to emptyFlow()))

    assertFailsWith<CancellationException> {
      exchangeTaskExecutor.execute(
        ExchangeStepAttemptKey("w", "x", "y", "z"),
        buildExchangeStep(
          name = "some-name",
          dataProviderName = "some-edp",
          modelProviderName = "some-mp",
          testedStep =
            step {
              encryptStep = encryptStep {}
              inputLabels["a"] = "b"
              outputLabels["Out:a"] = "c"
            }
        )
      )
    }

    assertFails { storage.verifiedBatchRead(mapOf("Out:a" to "c")) }
  }
}
