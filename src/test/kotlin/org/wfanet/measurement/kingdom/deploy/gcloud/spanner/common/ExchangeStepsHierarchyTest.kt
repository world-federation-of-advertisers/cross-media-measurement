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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.common.truth.Truth.assertThat
import com.google.type.Date
import java.lang.IllegalArgumentException
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangeDetails

private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val EXTERNAL_MODEL_PROVIDER_ID = 6L
private const val EXTERNAL_RECURRING_EXCHANGE_ID1 = 8L
private const val EXTERNAL_RECURRING_EXCHANGE_ID2 = 10L

private val DATE1 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 1
      day = 15
    }
    .build()
private val DATE2 =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 3
      day = 1
    }
    .build()

private val EXCHANGE_WORKFLOW1 =
  ExchangeWorkflow.newBuilder()
    .apply {
      addAllSteps(
        mutableListOf(
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 1
              addAllPrerequisiteStepIndices(emptyList())
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 2
              addAllPrerequisiteStepIndices(listOf(1))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 3
              addAllPrerequisiteStepIndices(listOf(1, 2))
            }
            .build()
        )
      )
    }
    .build()

private val EXCHANGE_WORKFLOW2 =
  ExchangeWorkflow.newBuilder()
    .apply {
      addAllSteps(
        mutableListOf(
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 1
              addAllPrerequisiteStepIndices(listOf(5))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 2
              addAllPrerequisiteStepIndices(listOf(1))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 3
              addAllPrerequisiteStepIndices(listOf(1, 2))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.DATA_PROVIDER
              stepIndex = 4
              addAllPrerequisiteStepIndices(listOf(3))
            }
            .build(),
          ExchangeWorkflow.Step.newBuilder()
            .apply {
              party = ExchangeWorkflow.Party.MODEL_PROVIDER
              stepIndex = 5
              addAllPrerequisiteStepIndices(listOf(4))
            }
            .build()
        )
      )
    }
    .build()

@RunWith(JUnit4::class)
class ExchangeStepsHierarchyTest {

  @Test
  fun `getReadyExchangeStep`() {
    val recurringExchange =
      RecurringExchange.newBuilder()
        .apply {
          this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          this.nextExchangeDate = DATE1
          this.state = RecurringExchange.State.ACTIVE
          this.details =
            RecurringExchangeDetails.newBuilder()
              .apply { exchangeWorkflow = EXCHANGE_WORKFLOW1 }
              .build()
        }
        .build()
    val hierarchy = ExchangeStepsHierarchy(recurringExchange)
    val firstActual = hierarchy.getReadyExchangeStep()
    val expected =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          date = DATE1
          stepIndex = 1
          state = ExchangeStep.State.READY
        }
        .build()

    assertThat(firstActual).isEqualTo(expected)
  }

  @Test
  fun `getReadyExchangeStep with input`() {
    val recurringExchange =
      RecurringExchange.newBuilder()
        .apply {
          this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID2
          this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          this.nextExchangeDate = DATE2
          this.state = RecurringExchange.State.ACTIVE
          this.details =
            RecurringExchangeDetails.newBuilder()
              .apply { exchangeWorkflow = EXCHANGE_WORKFLOW1 }
              .build()
        }
        .build()
    val expected1 =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          date = DATE2
          stepIndex = 1
          state = ExchangeStep.State.BLOCKED
        }
        .build()
    val expected2 =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          date = DATE2
          stepIndex = 2
          state = ExchangeStep.State.BLOCKED
        }
        .build()
    val expected3 =
      ExchangeStep.newBuilder()
        .apply {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          date = DATE2
          stepIndex = 3
          state = ExchangeStep.State.BLOCKED
        }
        .build()
    val hierarchy =
      ExchangeStepsHierarchy(
        recurringExchange = recurringExchange,
        exchangeSteps = listOf(expected1, expected2, expected3)
      )
    val firstActual = hierarchy.getReadyExchangeStep()

    assertThat(firstActual)
      .isEqualTo(expected1.toBuilder().setState(ExchangeStep.State.READY).build())

    val actualSteps = hierarchy.getExchangeSteps()
    assertThat(actualSteps)
      .isEqualTo(
        listOf(
          expected1.toBuilder().setState(ExchangeStep.State.READY).build(),
          expected2,
          expected3
        )
      )
  }

  @Test
  fun `getReadyExchangeStep with invalid workflow`() {
    val recurringExchange =
      RecurringExchange.newBuilder()
        .apply {
          this.externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID1
          this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          this.externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          this.nextExchangeDate = DATE1
          this.state = RecurringExchange.State.ACTIVE
          this.details =
            RecurringExchangeDetails.newBuilder()
              .apply { exchangeWorkflow = EXCHANGE_WORKFLOW2 }
              .build()
        }
        .build()

    val exception =
      assertFailsWith<IllegalArgumentException> { ExchangeStepsHierarchy(recurringExchange) }
    assertThat(exception).hasMessageThat().contains("Valid RecurringExchange and workflow needed.")
  }
}
