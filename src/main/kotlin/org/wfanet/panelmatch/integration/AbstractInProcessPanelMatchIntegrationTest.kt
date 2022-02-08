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
import com.google.common.truth.Truth.assertWithMessage
import com.google.privatemembership.batch.Shared
import com.google.protobuf.ByteString
import com.google.protobuf.TypeRegistry
import io.grpc.StatusException
import java.nio.file.Path
import java.time.Clock
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
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt.ExchangesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.integration.deploy.gcloud.buildKingdomSpannerEmulatorDatabaseRule
import org.wfanet.measurement.integration.deploy.gcloud.buildSpannerInProcessKingdom
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.testing.runBlockingTest

private val TERMINAL_STEP_STATES = setOf(ExchangeStep.State.SUCCEEDED, ExchangeStep.State.FAILED)
private val READY_STEP_STATES =
  setOf(
    ExchangeStep.State.IN_PROGRESS,
    ExchangeStep.State.READY,
    ExchangeStep.State.READY_FOR_RETRY
  )
private val TERMINAL_EXCHANGE_STATES = setOf(Exchange.State.SUCCEEDED, Exchange.State.FAILED)

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
    val typeRegistry = TypeRegistry.newBuilder().add(Shared.Parameters.getDescriptor()).build()
    checkNotNull(this::class.java.getResource(exchangeWorkflowResourcePath))
      .openStream()
      .use { input -> parseTextProto(input.bufferedReader(), exchangeWorkflow {}, typeRegistry) }
      .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
  }
  private val serializedExchangeWorkflow: ByteString by lazy { exchangeWorkflow.toByteString() }

  private val databaseRule = buildKingdomSpannerEmulatorDatabaseRule()
  private val inProcessKingdom by lazy {
    buildSpannerInProcessKingdom(databaseRule, Clock.systemUTC(), verboseGrpcLogging = false)
  }
  private val resourceSetup by lazy { inProcessKingdom.panelMatchResourceSetup }

  private lateinit var exchangesClient: ExchangesCoroutineStub
  private lateinit var exchangeStepsClient: ExchangeStepsCoroutineStub
  private lateinit var modelProviderContext: ProviderContext
  private lateinit var dataProviderContext: ProviderContext
  private lateinit var exchangeKey: ExchangeKey
  private lateinit var recurringExchangeId: String

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

  private fun makeExchangesServiceClient(principal: String): ExchangesCoroutineStub {
    return ExchangesCoroutineStub(inProcessKingdom.publicApiChannel).withPrincipalName(principal)
  }

  private fun makeExchangeStepsServiceClient(principal: String): ExchangeStepsCoroutineStub {
    return ExchangeStepsCoroutineStub(inProcessKingdom.publicApiChannel)
      .withPrincipalName(principal)
  }

  private suspend fun getSteps(): List<ExchangeStep> {
    return exchangeStepsClient.listExchangeSteps(
        listExchangeStepsRequest {
          parent = exchangeKey.toName()
          pageSize = 50
          filter = filter { exchangeDates += EXCHANGE_DATE.toProtoDate() }
        }
      )
      .exchangeStepList
      .sortedBy { step -> step.stepIndex }
  }

  private fun logStepStates(steps: Iterable<ExchangeStep>) {
    val stepsList = exchangeWorkflow.stepsList
    val message = StringBuilder("ExchangeStep states:")
    for ((state, stepsForState) in steps.groupBy { it.state }) {
      val stepsString =
        stepsForState.sortedBy { it.stepIndex }.joinToString(", ") {
          "${stepsList[it.stepIndex].stepId}#${it.stepIndex}"
        }
      message.appendLine("  $state: $stepsString")
    }
    logger.info(message.toString())
  }

  private fun assertNotDeadlocked(steps: Iterable<ExchangeStep>) {
    if (steps.any { it.state !in TERMINAL_STEP_STATES }) {
      assertThat(steps.any { it.state in READY_STEP_STATES }).isTrue()
    }
  }

  private suspend fun isDone(): Boolean {
    val modelProviderId = requireNotNull(exchangeKey.modelProviderId)
    val request = getExchangeRequest {
      name = exchangeKey.toName()
      modelProvider = ModelProviderKey(modelProviderId).toName()
    }

    return try {
      val exchange = exchangesClient.getExchange(request)

      val steps = getSteps()
      logStepStates(steps)
      assertNotDeadlocked(steps)

      logger.info("Exchange is in state: ${exchange.state}.")
      exchange.state in TERMINAL_EXCHANGE_STATES
    } catch (e: StatusException) {
      false
    }
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

  @Before
  fun setup() = runBlocking {
    val keys =
      resourceSetup.createResourcesForWorkflow(
        SCHEDULE,
        API_VERSION,
        exchangeWorkflow,
        EXCHANGE_DATE.toProtoDate(),
      )
    exchangesClient = makeExchangesServiceClient(keys.modelProviderKey.toName())
    exchangeStepsClient = makeExchangeStepsServiceClient(keys.modelProviderKey.toName())
    recurringExchangeId = keys.recurringExchangeKey.recurringExchangeId
    exchangeKey =
      ExchangeKey(
        null,
        keys.modelProviderKey.modelProviderId,
        recurringExchangeId = recurringExchangeId,
        exchangeId = EXCHANGE_DATE.toString()
      )
    dataProviderContext =
      ProviderContext(
        keys.dataProviderKey,
        dataProviderFolder.root.toPath(),
        createScope("EDP_SCOPE")
      )
    modelProviderContext =
      ProviderContext(
        keys.modelProviderKey,
        modelProviderFolder.root.toPath(),
        createScope("MP_SCOPE")
      )
  }

  @Test
  fun runTest() = runBlockingTest {
    val exchangeDateKey = ExchangeDateKey(recurringExchangeId, EXCHANGE_DATE)
    val dataProviderDaemon = makeDaemon(dataProviderContext, modelProviderContext, exchangeDateKey)
    val modelProviderDaemon = makeDaemon(modelProviderContext, dataProviderContext, exchangeDateKey)

    for ((blobKey, value) in initialDataProviderInputs) {
      dataProviderDaemon.writePrivateBlob(blobKey, value)
    }

    for ((blobKey, value) in initialModelProviderInputs) {
      modelProviderDaemon.writePrivateBlob(blobKey, value)
    }

    logger.info("Shared Folder path: ${sharedFolder.root.absolutePath}")

    dataProviderDaemon.run()
    modelProviderDaemon.run()

    while (!isDone()) {
      delay(500)
    }

    dataProviderContext.scope.cancel()
    modelProviderContext.scope.cancel()

    validateFinalState(dataProviderDaemon, modelProviderDaemon)

    val steps = getSteps()
    assertThat(steps.size == exchangeWorkflow.stepsCount)

    for (step in steps) {
      assertWithMessage("Step ${step.stepIndex}")
        .that(step.state)
        .isEqualTo(ExchangeStep.State.SUCCEEDED)
    }
  }

  companion object {
    private val logger by loggerFor()
  }
}
