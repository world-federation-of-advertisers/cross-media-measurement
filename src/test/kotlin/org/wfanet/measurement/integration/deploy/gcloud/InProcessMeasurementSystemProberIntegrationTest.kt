/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.deploy.gcloud

import java.io.File
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.MC_DISPLAY_NAME
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.kingdom.batch.MeasurementSystemProber
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessMeasurementSystemProberIntegrationTest(
  // need these

  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
) {
  abstract val kingdomDataServicesRule: ProviderRule<DataServices>

  private val kingdomDataServices: DataServices
    get() = kingdomDataServicesRule.value

  //  private val kingdom: InProcessKingdom =
  //    InProcessKingdom(
  //      dataServicesProvider = { kingdomDataServices },
  //      REDIRECT_URI,
  //      verboseGrpcLogging = false,
  //    )

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule)

  private lateinit var prober: MeasurementSystemProber

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
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

  private val publicAccountsClient by lazy {
    AccountsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicApiKeysClient by lazy {
    ApiKeysCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private lateinit var mcResourceName: String
  private lateinit var edpDisplayName: String
  private lateinit var edpResourceName: String

  @Before
  fun startDaemons() {
    inProcessCmmsComponents.startDaemons()
    initMeasurementSystemProber()
  }

  @Before
  fun initMeasurementSystemProber() = runBlocking {
    //    val resourceSetup =
    //      ResourceSetup(
    //        internalAccountsClient = inProcessCmmsComponents.kingdom.internalAccountsClient,
    //        internalDataProvidersClient =
    // inProcessCmmsComponents.kingdom.internalDataProvidersClient,
    //        accountsClient = publicAccountsClient,
    //        apiKeysClient = publicApiKeysClient,
    //        internalCertificatesClient =
    // inProcessCmmsComponents.kingdom.internalCertificatesClient,
    //        measurementConsumersClient = publicMeasurementConsumersClient,
    //        runId = "67890",
    //        requiredDuchies = listOf(),
    //      )
    //    // Create the MC.
    //    val measurementConsumer =
    //      resourceSetup
    //        .createMeasurementConsumer(MC_ENTITY_CONTENT,
    // resourceSetup.createAccountWithRetries())
    //        .measurementConsumer

    //    mcResourceName = measurementConsumer.name
    //    // Create EDP Resource
    //    edpDisplayName = ALL_EDP_DISPLAY_NAMES[0]
    //    val internalDataProvider =
    //      resourceSetup.createInternalDataProvider(createEntityContent(edpDisplayName))
    //    val dataProviderId = externalIdToApiId(internalDataProvider.externalDataProviderId)
    //    edpResourceName = DataProviderKey(dataProviderId).toName()

    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    prober =
      MeasurementSystemProber(
        measurementConsumerData.name,
        inProcessCmmsComponents.edpSimulators.map { it.displayName },
        measurementConsumerData.apiAuthenticationKey,
        PRIVATE_KEY_DER_FILE,
        MEASUREMENT_LOOKBACK_DURATION,
        DURATION_BETWEEN_MEASUREMENT,
        publicMeasurementConsumersClient,
        publicMeasurementsClient,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicRequisitionsClient,
        CLOCK,
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

  // one test case
  // create measurement
  // report metric

  // use EdPSimulator (dataProvider + eventGroup, requisition)
  @Test
  fun `run creates a new measurement when there are no previous measurements`(): Unit =
    runBlocking {
      prober.run()
    }

  companion object {
    private const val REDIRECT_URI = "https://localhost:2050"
    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
    private const val API_AUTHENTICATION_KEY = "some-api-key"
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    private val PRIVATE_KEY_DER_FILE = SECRETS_DIR.resolve("mc_cs_private.der")
    private val DURATION_BETWEEN_MEASUREMENT = Duration.ofDays(1)
    private val MEASUREMENT_LOOKBACK_DURATION = Duration.ofDays(1)
    private val CLOCK = Clock.systemUTC()
  }
}
