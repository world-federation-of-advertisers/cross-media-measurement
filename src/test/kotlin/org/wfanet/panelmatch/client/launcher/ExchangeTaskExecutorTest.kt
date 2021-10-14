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
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.launcher.testing.FakeTimeout
import org.wfanet.panelmatch.client.launcher.testing.buildExchangeStep
import org.wfanet.panelmatch.common.storage.createBlob
import org.wfanet.panelmatch.common.storage.toStringUtf8
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

@RunWith(JUnit4::class)
class ExchangeTaskExecutorTest {
  private val apiClient: ApiClient = mock()
  private val storage = InMemoryStorageClient()
  private val timeout = FakeTimeout()

  private val exchangeTask =
    object : ExchangeTask {
      override suspend fun execute(
        input: Map<String, StorageClient.Blob>
      ): Map<String, Flow<ByteString>> {
        return input.mapKeys { "Out:${it.key}" }.mapValues {
          val valString: String = it.value.toStringUtf8()
          "Out:$valString".toByteString().asBufferedFlow(1024)
        }
      }
    }

  private val exchangeTaskExecutor =
    ExchangeTaskExecutor(apiClient, timeout, storage) { exchangeTask }

  @Test
  fun `reads inputs and writes outputs`() = runBlockingTest {
    val blob = "some-blob".toByteString()

    storage.createBlob("b", blob)

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

    assertThat(storage.getBlob("c")?.toStringUtf8()).isEqualTo("Out:some-blob")
  }

  @Test
  fun timeout() = runBlockingTest {
    timeout.expired = true

    storage.createBlob("b", emptyFlow())

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

    assertThat(storage.getBlob("c")).isNull()
  }
}
