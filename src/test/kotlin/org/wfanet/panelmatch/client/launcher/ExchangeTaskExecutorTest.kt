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
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.encryptStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidatedExchangeStep
import org.wfanet.panelmatch.client.launcher.testing.FakeTimeout
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.client.storage.testing.makeTestPrivateStorageSelector
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.storage.toStringUtf8
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

// TODO: move somewhere for reuse
class TestPrivateStorageSelector {
  val storageDetailsMap = mutableMapOf<String, ByteString>()
  val blobs = ConcurrentHashMap<String, StorageClient.Blob>()
  val storageClient = InMemoryStorageClient(blobs)
  private val secrets = TestSecretMap(storageDetailsMap)
  val selector = makeTestPrivateStorageSelector(secrets, storageClient)
}

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private val ATTEMPT_KEY = ExchangeStepAttemptKey(RECURRING_EXCHANGE_ID, "x", "y", "z")

private val DATE = LocalDate.of(2021, 11, 3)

private val WORKFLOW = exchangeWorkflow {
  steps +=
    step {
      encryptStep = encryptStep {}
      inputLabels["a"] = "b"
      outputLabels["Out:a"] = "c"
    }
}

private val VALIDATED_EXCHANGE_STEP = ValidatedExchangeStep(WORKFLOW, WORKFLOW.getSteps(0), DATE)

@RunWith(JUnit4::class)
class ExchangeTaskExecutorTest {
  private val testPrivateStorageSelector = TestPrivateStorageSelector()
  private val apiClient: ApiClient = mock()
  private val timeout = FakeTimeout()

  private val storageDetails = storageDetails {
    gcs = StorageDetailsKt.gcsStorage {}
    visibility = StorageDetails.Visibility.PRIVATE
  }

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

  private val exchangeTaskMapper =
    object : ExchangeTaskMapper {
      override suspend fun getExchangeTaskForStep(context: ExchangeContext): ExchangeTask {
        return exchangeTask
      }
    }

  private val exchangeTaskExecutor =
    ExchangeTaskExecutor(
      apiClient,
      timeout,
      testPrivateStorageSelector.selector,
      exchangeTaskMapper
    )

  @Before
  fun setUpStorage() {
    testPrivateStorageSelector.storageDetailsMap[RECURRING_EXCHANGE_ID] =
      storageDetails.toByteString()
  }

  @Test
  fun `reads inputs and writes outputs`() = runBlockingTest {
    val blob = "some-blob".toByteString()

    testPrivateStorageSelector.storageClient.createBlob("b", blob.asBufferedFlow(1024))

    exchangeTaskExecutor.execute(VALIDATED_EXCHANGE_STEP, ATTEMPT_KEY)

    assertThat(testPrivateStorageSelector.storageClient.getBlob("c")?.toStringUtf8())
      .isEqualTo("Out:some-blob")
  }

  @Test
  fun timeout() = runBlockingTest {
    timeout.expired = true

    assertFailsWith<CancellationException> {
      exchangeTaskExecutor.execute(VALIDATED_EXCHANGE_STEP, ATTEMPT_KEY)
    }

    assertThat(testPrivateStorageSelector.storageClient.getBlob("c")).isNull()
  }
}
