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

package org.wfanet.panelmatch.integration

import com.google.common.truth.Truth.assertThat
import com.google.type.Date
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.integration.deploy.gcloud.buildKingdomSpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.deploy.gcloud.buildSpannerInProcessKingdom
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.panelmatch.client.storage.testing.makeTestPrivateStorageSelector
import org.wfanet.panelmatch.client.storage.testing.makeTestSharedStorageSelector
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.storage.createBlob
import org.wfanet.panelmatch.common.toByteString

private const val SCHEDULE = "@daily"
private const val API_VERSION = "v2alpha"
private val CLOCK: Clock = Clock.systemUTC()
private val TODAY: Date = LocalDate.now().toProtoDate()
private val HKDF_PEPPER = "hkdf-pepper".toByteStringUtf8()
private val TASK_TIMEOUT_DURATION = Duration.ofSeconds(3)
private val POLLING_INTERVAL = Duration.ofSeconds(3)
private val DAEMONS_DELAY_DURATION = Duration.ofSeconds(1).toMillis()

/** E2E Test for Panel Match that everything is wired up and working properly. */
@RunWith(JUnit4::class)
class InProcessPanelMatchIntegrationTest {
  private val databaseRule = buildKingdomSpannerEmulatorDatabaseRule()
  private val inProcessKingdom = buildSpannerInProcessKingdom(databaseRule, CLOCK)
  private val resourceSetup = inProcessKingdom.panelMatchResourceSetup

  @get:Rule val temporaryFolder = TemporaryFolder()
  @get:Rule
  val ruleChain: TestRule by lazy { chainRulesSequentially(databaseRule, inProcessKingdom) }

  private lateinit var dataProviderKey: DataProviderKey
  private lateinit var modelProviderKey: ModelProviderKey
  private lateinit var recurringExchangeKey: RecurringExchangeKey

  private fun createScope(name: String): CoroutineScope {
    return CoroutineScope(CoroutineName(name + Dispatchers.Default))
  }

  @Before
  fun setup() = runBlocking {
    val providers =
      resourceSetup.createResourcesForWorkflow(
        exchangeSchedule = SCHEDULE,
        apiVersion = API_VERSION,
        exchangeWorkflow = exchangeWorkflow,
        exchangeDate = TODAY
      )
    dataProviderKey = providers.dataProviderKey
    modelProviderKey = providers.modelProviderKey
    recurringExchangeKey = providers.recurringExchangeKey
  }

  @Ignore // TODO: this test is still a work-in-progress.
  @Test
  fun `entire process`() = runBlocking {
    val recurringExchangeId = recurringExchangeKey.recurringExchangeId
    val edpSecretMap = TestSecretMap(mapOf(recurringExchangeId to exchangeWorkflow.toByteString()))
    val mpSecretMap = TestSecretMap(mapOf(recurringExchangeId to exchangeWorkflow.toByteString()))

    // TODO(@yunyeng): Build storage from InputBlobs map.
    val edpStorageClient = InMemoryStorageClient()
    val mpStorageClient = InMemoryStorageClient()
    edpStorageClient.createBlob("edp-hkdf-pepper", HKDF_PEPPER)

    val edpScope = createScope("EDP SCOPE")
    val edpDaemon =
      ExchangeWorkflowDaemonForTest(
        privateStorageSelector = makeTestPrivateStorageSelector(edpSecretMap, edpStorageClient),
        sharedStorageSelector = makeTestSharedStorageSelector(edpSecretMap, edpStorageClient),
        clock = CLOCK,
        scope = edpScope,
        validExchangeWorkflows = edpSecretMap,
        channel = inProcessKingdom.publicApiChannel,
        providerKey = dataProviderKey,
        taskTimeoutDuration = TASK_TIMEOUT_DURATION,
        pollingInterval = POLLING_INTERVAL,
        rootCertificates = secretMap,
        privateKeys = secretMap,
        privateStorageFactories = emptyMap(),
        privateStorageInformation = secretMap,
        sharedStorageFactories = emptyMap(),
        sharedStorageInformation = secretMap,
      )

    val mpScope = createScope("MP SCOPE")
    val mpDaemon =
      ExchangeWorkflowDaemonForTest(
        privateStorageSelector = makeTestPrivateStorageSelector(mpSecretMap, mpStorageClient),
        sharedStorageSelector = makeTestSharedStorageSelector(mpSecretMap, mpStorageClient),
        clock = CLOCK,
        scope = mpScope,
        validExchangeWorkflows = mpSecretMap,
        channel = inProcessKingdom.publicApiChannel,
        providerKey = modelProviderKey,
        taskTimeoutDuration = TASK_TIMEOUT_DURATION,
        pollingInterval = POLLING_INTERVAL,
        rootCertificates = secretMap,
        privateKeys = secretMap,
        privateStorageFactories = emptyMap(),
        privateStorageInformation = secretMap,
        sharedStorageFactories = emptyMap(),
        sharedStorageInformation = secretMap,
      )
    edpDaemon.run()
    mpDaemon.run()
    delay(DAEMONS_DELAY_DURATION)
    edpScope.cancel()
    mpScope.cancel()

    assertThat(edpDaemon.apiClient.claimExchangeStep()).isNull()
  }

  companion object {
    private val exchangeWorkflow: ExchangeWorkflow

    init {
      val configPath = "config/mini_exchange_workflow.textproto"
      val resource = this::class.java.getResource(configPath)

      exchangeWorkflow =
        resource
          .openStream()
          .use { input ->
            parseTextProto(input.bufferedReader(), ExchangeWorkflow.getDefaultInstance())
          }
          // TODO(@yunyeng): Think about the tests that start running around midnight.
          .copy { this.firstExchangeDate = TODAY }
    }
  }
}
