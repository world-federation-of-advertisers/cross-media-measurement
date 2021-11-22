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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.Date
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.integration.deploy.gcloud.buildKingdomSpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.deploy.gcloud.buildSpannerInProcessKingdom
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val SCHEDULE = "@daily"
private const val API_VERSION = "v2alpha"
private val TODAY: Date = LocalDate.now().toProtoDate()
private val TEST_TIMEOUT = Duration.ofSeconds(10)

private val HKDF_PEPPER = "some-hkdf-pepper".toByteStringUtf8()

/** E2E Test for Panel Match that everything is wired up and working properly. */
@RunWith(JUnit4::class)
class InProcessPanelMatchIntegrationTest {
  private val databaseRule = buildKingdomSpannerEmulatorDatabaseRule()
  private val inProcessKingdom by lazy {
    buildSpannerInProcessKingdom(databaseRule, Clock.systemUTC(), verboseGrpcLogging = true)
  }
  private val resourceSetup by lazy { inProcessKingdom.panelMatchResourceSetup }

  @get:Rule
  val ruleChain: TestRule by lazy { chainRulesSequentially(databaseRule, inProcessKingdom) }

  @get:Rule val dataProviderFolder = TemporaryFolder()
  @get:Rule val modelProviderFolder = TemporaryFolder()
  @get:Rule val sharedFolder = TemporaryFolder()

  private data class ProviderContext(
    val key: ResourceKey,
    val privateStoragePath: Path,
    val scope: CoroutineScope
  )

  private lateinit var dataProviderContext: ProviderContext
  private lateinit var modelProviderContext: ProviderContext
  private lateinit var exchangeDateKey: ExchangeDateKey

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
    exchangeDateKey =
      ExchangeDateKey(providers.recurringExchangeKey.recurringExchangeId, TODAY.toLocalDate())

    dataProviderContext =
      ProviderContext(
        providers.dataProviderKey,
        dataProviderFolder.root.toPath(),
        createScope("EDP_SCOPE")
      )

    modelProviderContext =
      ProviderContext(
        providers.modelProviderKey,
        modelProviderFolder.root.toPath(),
        createScope("MP_SCOPE")
      )
  }

  private fun makeDaemon(
    owner: ProviderContext,
    partner: ProviderContext
  ): ExchangeWorkflowDaemonForTest {
    return ExchangeWorkflowDaemonForTest(
      v2alphaChannel = inProcessKingdom.publicApiChannel,
      provider = owner.key,
      partnerProvider = partner.key,
      exchangeDateKey = exchangeDateKey,
      serializedExchangeWorkflow = exchangeWorkflow.toByteString(),
      privateDirectory = owner.privateStoragePath,
      sharedDirectory = sharedFolder.root.toPath(),
      scope = owner.scope
    )
  }

  @Test
  fun miniWorkflow() = runBlockingTest {
    val dataProviderDaemon = makeDaemon(dataProviderContext, modelProviderContext)
    val modelProviderDaemon = makeDaemon(modelProviderContext, dataProviderContext)

    dataProviderDaemon.writePrivateBlob("edp-hkdf-pepper", HKDF_PEPPER)

    dataProviderDaemon.run()
    modelProviderDaemon.run()

    var blob: ByteString? = null
    val start = Instant.now()
    while (Duration.between(start, Instant.now()) < TEST_TIMEOUT && blob == null) {
      blob = modelProviderDaemon.readPrivateBlob("mp-hkdf-pepper")
      delay(250)
    }

    dataProviderContext.scope.cancel()
    modelProviderContext.scope.cancel()

    assertThat(blob).isEqualTo(HKDF_PEPPER)
  }

  companion object {
    private val exchangeWorkflow: ExchangeWorkflow

    init {
      val configPath = "config/mini_exchange_workflow.textproto"
      val resource = requireNotNull(this::class.java.getResource(configPath))

      exchangeWorkflow =
        resource
          .openStream()
          .use { input ->
            parseTextProto(input.bufferedReader(), ExchangeWorkflow.getDefaultInstance())
          }
          // TODO(@yunyeng): Think about the tests that start running around midnight.
          .copy { firstExchangeDate = TODAY }
    }
  }
}
