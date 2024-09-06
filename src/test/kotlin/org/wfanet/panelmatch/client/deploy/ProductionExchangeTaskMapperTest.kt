// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.deploy

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.deploy.testing.TestProductionExchangeTaskMapper
import org.wfanet.panelmatch.client.exchangetasks.CopyToSharedStorageTask
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.commutativeDeterministicEncryptStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyOptions
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyToSharedStorageStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.client.launcher.testing.inputStep
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt.fileStorage
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.client.storage.testing.TestPrivateStorageSelector
import org.wfanet.panelmatch.client.storage.testing.TestSharedStorageSelector
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ProductionExchangeTaskMapperTest {
  private val testPrivateStorageSelector = TestPrivateStorageSelector()
  private val testSharedStorageSelector = TestSharedStorageSelector()
  private val exchangeTaskMapper =
    TestProductionExchangeTaskMapper(
      testPrivateStorageSelector.selector,
      testSharedStorageSelector.selector,
    )

  private val testPrivateStorageDetails = storageDetails {
    file = fileStorage {}
    visibility = StorageDetails.Visibility.PRIVATE
  }
  private val testSharedStorageDetails = storageDetails {
    file = fileStorage {}
    visibility = StorageDetails.Visibility.SHARED
  }

  @Before
  fun setUpStorageDetails() {
    testPrivateStorageSelector.storageDetails.underlyingMap[RECURRING_EXCHANGE_ID] =
      testPrivateStorageDetails.toByteString()
    testSharedStorageSelector.storageDetails.underlyingMap[RECURRING_EXCHANGE_ID] =
      testSharedStorageDetails.toByteString()
  }

  @Test
  fun `map export task with dependent input`() = runBlockingTest {
    val context = ExchangeContext(ATTEMPT_KEY, DATE, WORKFLOW, WORKFLOW.getSteps(2))
    val exchangeTask = exchangeTaskMapper.getExchangeTaskForStep(context)
    assertThat(exchangeTask).isInstanceOf(CopyToSharedStorageTask::class.java)
  }

  @Test
  fun `map export task without dependent input`() = runBlockingTest {
    val context = ExchangeContext(ATTEMPT_KEY, DATE, WORKFLOW, WORKFLOW.getSteps(3))
    val exchangeTask = exchangeTaskMapper.getExchangeTaskForStep(context)
    assertThat(exchangeTask).isInstanceOf(CopyToSharedStorageTask::class.java)
  }

  @Test
  fun `map export task with too many inputs`() = runBlockingTest {
    val context = ExchangeContext(ATTEMPT_KEY, DATE, WORKFLOW, WORKFLOW.getSteps(4))
    assertFailsWith<IllegalArgumentException> { exchangeTaskMapper.getExchangeTaskForStep(context) }
  }

  companion object {
    private val WORKFLOW = exchangeWorkflow {
      steps += inputStep("a" to "b")
      steps += step {
        this.commutativeDeterministicEncryptStep = commutativeDeterministicEncryptStep {}
      }
      steps += step {
        this.copyToSharedStorageStep = copyToSharedStorageStep {
          this.copyOptions = copyOptions {
            labelType = ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
          }
        }
        inputLabels.put("certificate-resource-name", "edp-certificate-resource-name")
        inputLabels.put("hkdf-pepper", "edp-hkdf-pepper")
        outputLabels.put("hkdf-pepper", "hkdf-pepper")
      }
      steps += step {
        this.copyToSharedStorageStep = copyToSharedStorageStep {
          this.copyOptions = copyOptions {
            labelType = ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
          }
        }
        inputLabels.put("hkdf-pepper", "edp-hkdf-pepper")
        outputLabels.put("hkdf-pepper", "hkdf-pepper")
      }
      steps += step {
        this.copyToSharedStorageStep = copyToSharedStorageStep {
          this.copyOptions = copyOptions {
            labelType = ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
          }
        }
        inputLabels.put("hkdf-pepper-1", "edp-hkdf-pepper-1")
        inputLabels.put("hkdf-pepper-2", "edp-hkdf-pepper-2")
        outputLabels.put("hkdf-pepper", "hkdf-pepper")
      }
    }

    private val DATE: LocalDate = LocalDate.of(2021, 11, 1)

    private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
    private val ATTEMPT_KEY =
      ExchangeStepAttemptKey(RECURRING_EXCHANGE_ID, "some-exchange", "some-step", "some-attempt")
  }
}
