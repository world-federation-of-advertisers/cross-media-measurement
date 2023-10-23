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

import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
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
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>
) {

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule)

  private lateinit var mcSimulator: MeasurementConsumerSimulator

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
  private val publicRequisitionsClient by lazy {
    RequisitionsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  @Before
  fun startDaemons() {
    inProcessCmmsComponents.startDaemons()
    initMcSimulator()
  }

  private fun initMcSimulator() {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventQuery =
      MetadataSyntheticGeneratorEventQuery(
        SyntheticGenerationSpecs.POPULATION_SPEC,
        InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY
      )
    mcSimulator =
      MeasurementConsumerSimulator(
        MeasurementConsumerData(
          measurementConsumerData.name,
          InProcessCmmsComponents.MC_ENTITY_CONTENT.signingKey,
          InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
          measurementConsumerData.apiAuthenticationKey
        ),
        OUTPUT_DP_PARAMS,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicMeasurementConsumersClient,
        publicCertificatesClient,
        RESULT_POLLING_DELAY,
        InProcessCmmsComponents.TRUSTED_CERTIFICATES,
        eventQuery,
        NoiseMechanism.CONTINUOUS_GAUSSIAN
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

  @Test
  fun `create a RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachAndFrequency("1234")
    }

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachAndFrequency("1234")
    }

  @Test
  fun `create a reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachOnly("1234")
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
  fun `create a RF measurement of invalid params and check the result contains error info`() =
    runBlocking {
      // Use frontend simulator to create an invalid reach and frequency measurement and verify
      // its error info.
      mcSimulator.testInvalidReachAndFrequency("1234")
    }

  // TODO(@renjiez): Add Multi-round test given the same input to verify correctness.

  companion object {
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1.0
    }
    private val RESULT_POLLING_DELAY = Duration.ofSeconds(10)

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
