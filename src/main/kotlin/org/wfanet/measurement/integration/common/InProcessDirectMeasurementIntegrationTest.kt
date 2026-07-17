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

package org.wfanet.measurement.integration.common

import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.dateInterval
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessDirectMeasurementIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
) :
  InProcessLifeOfAMeasurementIntegrationTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    hmssEnabled = false,
    trusTeeEnabled = false,
  ) {

  private val publicModelSuitesClient by lazy {
    ModelSuitesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
  }

  private val publicModelLinesClient by lazy {
    ModelLinesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
  }

  private val publicModelReleasesClient by lazy {
    ModelReleasesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
  }

  private val publicModelRolloutClient by lazy {
    ModelRolloutsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
      .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
  }

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testDirectReachAndFrequency("1234", 1)
    }

  @Test
  fun `create a direct reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testDirectReachOnly("1234", 1)
    }

  @Test
  fun `create an impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testImpression("1234")
    }

  @Test
  fun `create a duration measurement and check the result is equal to the expected result`() =
    runBlocking {
      mcSimulator.testDuration("1234")
    }

  @Test
  fun `create a population measurement`() = runBlocking {
    val modelSuite =
      publicModelSuitesClient.createModelSuite(
        createModelSuiteRequest {
          parent = inProcessCmmsComponents.modelProviderResourceName
          modelSuite = modelSuite { displayName = MODEL_SUITE_DISPLAY_NAME }
        }
      )
    val modelLine =
      publicModelLinesClient.createModelLine(
        createModelLineRequest {
          parent = modelSuite.name
          modelLine = modelLine {
            activeStartTime = MODEL_LINE_ACTIVE_START_TIME
            type = ModelLine.Type.PROD
          }
        }
      )

    val modelRelease =
      publicModelReleasesClient.createModelRelease(
        createModelReleaseRequest {
          parent = modelSuite.name
          modelRelease = modelRelease {
            population = inProcessCmmsComponents.populationResourceName
          }
        }
      )

    publicModelRolloutClient.createModelRollout(
      createModelRolloutRequest {
        parent = modelLine.name
        modelRollout = modelRollout {
          this.modelRelease = modelRelease.name
          gradualRolloutPeriod = dateInterval {
            startDate = ROLLOUT_PERIOD_START_DATE
            endDate = ROLLOUT_PERIOD_END_DATE
          }
        }
      }
    )

    val populationData = inProcessCmmsComponents.getPopulationData()
    mcSimulator.testPopulation(
      "1234",
      populationData,
      modelLine.name,
      DEFAULT_POPULATION_FILTER_EXPRESSION,
      TestEvent.getDescriptor(),
    )
  }

  companion object {
    private const val MODEL_SUITE_DISPLAY_NAME = "ModelSuite1"
    private val MODEL_LINE_ACTIVE_START_TIME = Instant.now().plusSeconds(2000L).toProtoTime()
    private const val DEFAULT_POPULATION_FILTER_EXPRESSION =
      "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
    private val ROLLOUT_PERIOD_START_DATE = LocalDate.now().plusDays(10).toProtoDate()
    private val ROLLOUT_PERIOD_END_DATE = LocalDate.now().plusDays(20).toProtoDate()
  }
}
