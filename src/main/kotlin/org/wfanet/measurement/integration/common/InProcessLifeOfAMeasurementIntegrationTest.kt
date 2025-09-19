// Copyright 2020 The Cross-Media Measurement Authors
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
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.dateInterval
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
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
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAMeasurementIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
) {

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule, useEdpSimulators = true)

  private lateinit var mcSimulator: EventQueryMeasurementConsumerSimulator

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicCertificatesClient by lazy {
    CertificatesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

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

  @Before
  fun startDaemons() {
    inProcessCmmsComponents.startDaemons()
    initMcSimulator()
  }

  private fun initMcSimulator() {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    mcSimulator =
      EventQueryMeasurementConsumerSimulator(
        MeasurementConsumerData(
          measurementConsumerData.name,
          InProcessCmmsComponents.MC_ENTITY_CONTENT.signingKey,
          InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
          measurementConsumerData.apiAuthenticationKey,
        ),
        OUTPUT_DP_PARAMS,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicMeasurementConsumersClient,
        publicCertificatesClient,
        InProcessCmmsComponents.TRUSTED_CERTIFICATES,
        inProcessCmmsComponents.eventQuery,
        NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
  }

  @After
  fun stopEdpSimulators() {
    inProcessCmmsComponents.stopEdpSimulators()
  }

  @After
  fun stopDuchyDaemons() {
    inProcessCmmsComponents.stopDuchyDaemons()
  }

  @After
  fun stopPopulationRequisitionFulfillerDaemon() {
    inProcessCmmsComponents.stopPopulationRequisitionFulfillerDaemon()
  }

  @Test
  fun `create a Llv2 RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachAndFrequency(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
      )
    }

  @Test
  fun `create a Hmss RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachAndFrequency(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
      )
    }

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachAndFrequency("1234", 1)
    }

  @Test
  fun `create a direct reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach-only measurement and verify its result.
      mcSimulator.testDirectReachOnly("1234", 1)
    }

  @Test
  fun `create a Llv2 reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachOnly(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
      )
    }

  @Test
  fun `create a Hmss reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachOnly(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
      )
    }

  @Test
  fun `create an impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its result.
      mcSimulator.testImpression("1234")
    }

  @Test
  fun `create a duration measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a duration measurement and verify its result.
      mcSimulator.testDuration("1234")
    }

  @Test
  fun `create a Llv2 RF measurement of invalid params and check the result contains error info`() =
    runBlocking {
      // Use frontend simulator to create an invalid reach and frequency measurement and verify
      // its error info.
      mcSimulator.testInvalidReachAndFrequency(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
      )
    }

  @Test
  fun `create a Hmss RF measurement of invalid params and check the result contains error info`() =
    runBlocking {
      // Use frontend simulator to create an invalid reach and frequency measurement and verify
      // its error info.
      mcSimulator.testInvalidReachAndFrequency(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
      )
    }

  @Test
  fun `create a Hmss RF measurement with wrapping vid interval and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachAndFrequency(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
        vidSamplingInterval {
          start = 0.5f
          width = 1.0f
        },
      )
    }

  // TODO(@renjiez): Add Multi-round test given the same input to verify correctness.

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

    // Use frontend simulator to create a population measurement
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
    // Epsilon can vary from 0.0001 to 1.0, delta = 1e-15 is a realistic value.
    // Set epsilon higher without exceeding privacy budget so the noise is smaller in the
    // integration test. Check sample values in CompositionTest.kt.
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1e-15
    }

    private const val MODEL_SUITE_DISPLAY_NAME = "ModelSuite1"

    private val MODEL_LINE_ACTIVE_START_TIME = Instant.now().plusSeconds(2000L).toProtoTime()

    private const val DEFAULT_POPULATION_FILTER_EXPRESSION =
      "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"

    private val ROLLOUT_PERIOD_START_DATE = LocalDate.now().plusDays(10).toProtoDate()

    private val ROLLOUT_PERIOD_END_DATE = LocalDate.now().plusDays(20).toProtoDate()

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
