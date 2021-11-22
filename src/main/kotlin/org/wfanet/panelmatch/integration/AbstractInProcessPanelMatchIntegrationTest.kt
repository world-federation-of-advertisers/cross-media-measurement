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

import com.google.protobuf.ByteString
import java.nio.file.Path
import java.time.Clock
import java.time.LocalDate
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.integration.deploy.gcloud.buildKingdomSpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.deploy.gcloud.buildSpannerInProcessKingdom
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val API_VERSION = "v2alpha"
private const val SCHEDULE = "@daily"

// TODO(@yunyeng): Think about the tests that start running around midnight.
private val EXCHANGE_DATE = LocalDate.now()

/** Base class to run a full, in-process end-to-end test of an ExchangeWorkflow. */
abstract class AbstractInProcessPanelMatchIntegrationTest {
  protected abstract val exchangeWorkflowResourcePath: String
  protected abstract val initialDataProviderInputs: Map<String, ByteString>
  protected abstract val initialModelProviderInputs: Map<String, ByteString>

  /** This is responsible for making an assertions about the final state of the workflow. */
  protected abstract fun validateFinalState(
    dataProviderDaemon: ExchangeWorkflowDaemonForTest,
    modelProviderDaemon: ExchangeWorkflowDaemonForTest
  )

  private val exchangeWorkflow: ExchangeWorkflow by lazy {
    checkNotNull(this::class.java.getResource(exchangeWorkflowResourcePath))
      .openStream()
      .use { input -> parseTextProto(input.bufferedReader(), exchangeWorkflow {}) }
      .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
  }
  private val serializedExchangeWorkflow: ByteString by lazy { exchangeWorkflow.toByteString() }

  private val databaseRule = buildKingdomSpannerEmulatorDatabaseRule()
  private val inProcessKingdom by lazy {
    buildSpannerInProcessKingdom(databaseRule, Clock.systemUTC(), verboseGrpcLogging = false)
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

  private suspend fun isDone(): Boolean {
    // TODO(@yunyeng): call /Exchanges.GetExchange to get Exchange status.
    delay(5000)
    return true
  }

  private fun createScope(name: String): CoroutineScope {
    return CoroutineScope(CoroutineName(name + Dispatchers.Default))
  }

  private fun makeDaemon(
    owner: ProviderContext,
    partner: ProviderContext,
    exchangeDateKey: ExchangeDateKey
  ): ExchangeWorkflowDaemonForTest {
    return ExchangeWorkflowDaemonForTest(
      v2alphaChannel = inProcessKingdom.publicApiChannel,
      provider = owner.key,
      partnerProvider = partner.key,
      exchangeDateKey = exchangeDateKey,
      serializedExchangeWorkflow = serializedExchangeWorkflow,
      privateDirectory = owner.privateStoragePath,
      sharedDirectory = sharedFolder.root.toPath(),
      scope = owner.scope
    )
  }

  @Test
  fun runTest() = runBlockingTest {
    val keys =
      resourceSetup.createResourcesForWorkflow(
        SCHEDULE,
        API_VERSION,
        exchangeWorkflow,
        EXCHANGE_DATE.toProtoDate(),
      )

    val recurringExchangeId = keys.recurringExchangeKey.recurringExchangeId
    val exchangeDateKey = ExchangeDateKey(recurringExchangeId, EXCHANGE_DATE)
    val dataProviderContext =
      ProviderContext(
        keys.dataProviderKey,
        dataProviderFolder.root.toPath(),
        createScope("EDP_SCOPE")
      )
    val modelProviderContext =
      ProviderContext(
        keys.modelProviderKey,
        modelProviderFolder.root.toPath(),
        createScope("MP_SCOPE")
      )

    val dataProviderDaemon = makeDaemon(dataProviderContext, modelProviderContext, exchangeDateKey)
    val modelProviderDaemon = makeDaemon(modelProviderContext, dataProviderContext, exchangeDateKey)

    for ((blobKey, value) in initialDataProviderInputs) {
      dataProviderDaemon.writePrivateBlob(blobKey, value)
    }

    for ((blobKey, value) in initialModelProviderInputs) {
      modelProviderDaemon.writePrivateBlob(blobKey, value)
    }

    dataProviderDaemon.run()
    modelProviderDaemon.run()

    while (!isDone()) {
      delay(500)
    }

    dataProviderContext.scope.cancel()
    modelProviderContext.scope.cancel()

    validateFinalState(dataProviderDaemon, modelProviderDaemon)
  }
}
