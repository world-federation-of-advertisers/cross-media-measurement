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

// import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.deploy.gcloud.SecureComputationServicesProviderRule
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.edpaggregator.MeasurementConsumerData
import org.wfanet.measurement.loadtest.edpaggregator.MeasurementConsumerSimulator
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  private val secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
) {

  private val inProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule)

  @JvmField val tempDirectory = TemporaryFolder()

  private val inProcessEdpAggregatorComponents: InProcessEdpAggregatorComponents = run {
    tempDirectory.create()
    val pubSubClient =
      GooglePubSubEmulatorClient(
        host = pubSubEmulatorProvider.host,
        port = pubSubEmulatorProvider.port,
      )
    InProcessEdpAggregatorComponents(
      internalServicesRule =
        SecureComputationServicesProviderRule(
          workItemPublisher = GoogleWorkItemPublisher(PROJECT_ID, pubSubClient),
          queueMapping = QueueMapping(QUEUES_CONFIG),
          emulatorDatabaseAdmin = secureComputationDatabaseAdmin,
        ),
      storagePath = tempDirectory.root.toPath(),
      pubSubClient = pubSubClient,
    )
  }

  @get:Rule
  val ruleChain: RuleChain =
    RuleChain.outerRule(inProcessCmmsComponents)
      .around(pubSubEmulatorProvider)
      .around(tempDirectory)
      .around(inProcessEdpAggregatorComponents)

  @Before
  fun setup() {
    inProcessCmmsComponents.startDaemons(useEdpSimulators = false)
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val edpDisplayNameToResourceMap = inProcessCmmsComponents.edpDisplayNameToResourceMap
    val kingdomChannel = inProcessCmmsComponents.kingdom.publicApiChannel
    inProcessEdpAggregatorComponents.startDaemons(
      kingdomChannel,
      measurementConsumerData,
      edpDisplayNameToResourceMap,
      "edp1",
    )
    initMcSimulator()
  }

  @Before fun createGooglePubSubEmulator() {}

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

  private fun initMcSimulator() {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    mcSimulator =
      MeasurementConsumerSimulator(
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
        TestEvent.getDefaultInstance(),
        NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
  }

  @After
  fun stopDuchyDaemons() {
    inProcessCmmsComponents.stopDuchyDaemons()
    inProcessEdpAggregatorComponents.stopDaemons()
  }

  @After
  fun stopPopulationRequisitionFulfillerDaemon() {
    inProcessCmmsComponents.stopPopulationRequisitionFulfillerDaemon()
  }

  @Ignore
  @Test
  fun `create a Llv2 RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachAndFrequency(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
      )
    }

  @Ignore
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
      withTimeout(40000) {
        delay(1000)
        // Use frontend simulator to create a direct reach and frequency measurement and verify its
        // result.
        mcSimulator.testDirectReachAndFrequency("1234")
      }
    }

  @Ignore
  @Test
  fun `create a direct reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      withTimeout(10000) {
        delay(1000)
        // Use frontend simulator to create a direct reach-only measurement and verify its result.
        mcSimulator.testDirectReachOnly("1234")
      }
    }

  @Ignore
  @Test
  fun `create a Llv2 reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachOnly(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
      )
    }

  @Ignore
  @Test
  fun `create a Hmss reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachOnly(
        "1234",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
      )
    }

  @Ignore
  @Test
  fun `create an impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its result.
      mcSimulator.testImpression("1234")
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    // Epsilon can vary from 0.0001 to 1.0, delta = 1e-15 is a realistic value.
    // Set epsilon higher without exceeding privacy budget so the noise is smaller in the
    // integration test. Check sample values in CompositionTest.kt.
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1e-15
    }

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()
  }
}
