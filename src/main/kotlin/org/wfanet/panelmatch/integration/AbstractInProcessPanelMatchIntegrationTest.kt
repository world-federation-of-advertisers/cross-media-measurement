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
import java.time.LocalDate
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeKey
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt.ExchangesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.InProcessKingdom
import org.wfanet.measurement.integration.deploy.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.client.tools.ConfigureResource
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.secrets.testing.TestMutableSecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.testing.FakeTinkKeyStorageProvider
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.testing.runBlockingTest

private val TERMINAL_STEP_STATES = setOf(ExchangeStep.State.SUCCEEDED, ExchangeStep.State.FAILED)
private val READY_STEP_STATES =
  setOf(
    ExchangeStep.State.IN_PROGRESS,
    ExchangeStep.State.READY,
    ExchangeStep.State.READY_FOR_RETRY,
  )
private val TERMINAL_EXCHANGE_STATES = setOf(Exchange.State.SUCCEEDED, Exchange.State.FAILED)

private const val SCHEDULE = "@daily"

/** Base class to run a full, in-process end-to-end test of an ExchangeWorkflow. */
abstract class AbstractInProcessPanelMatchIntegrationTest {
  protected abstract val exchangeWorkflowResourcePath: String
  protected abstract val initialDataProviderInputs: Map<String, ByteString>
  protected abstract val initialModelProviderInputs: Map<String, ByteString>
  open val initialSharedInputs: MutableMap<String, ByteString> = mutableMapOf()
  open val finalSharedOutputs: Map<String, ByteString> = emptyMap()

  /** This is responsible for making an assertions about the final state of the workflow. */
  protected abstract fun validateFinalState(
    dataProviderDaemon: ExchangeWorkflowDaemonForTest,
    modelProviderDaemon: ExchangeWorkflowDaemonForTest,
  )

  /** Used to validate the state of shared storage. */
  private suspend fun validateSharedStorageState(
    specialSharedStorageSelector: PrivateStorageSelector,
    exchangeDateKey: ExchangeDateKey,
  ) {
    for ((key, value) in finalSharedOutputs) {
      assertThat(specialSharedStorageSelector.readSharedBlob(key, exchangeDateKey)).isEqualTo(value)
    }
  }

  abstract val workflow: ExchangeWorkflow

  private val kingdomDataServicesProvider = KingdomDataServicesProviderRule(spannerEmulator)
  private val inProcessKingdom =
    InProcessKingdom({ kingdomDataServicesProvider.value }, REDIRECT_URI)
  private val resourceSetup by lazy { inProcessKingdom.panelMatchResourceSetup }

  private lateinit var exchangesClient: ExchangesCoroutineStub
  private lateinit var exchangeStepsClient: ExchangeStepsCoroutineStub
  private lateinit var modelProviderContext: ProviderContext
  private lateinit var dataProviderContext: ProviderContext
  private lateinit var exchangeKey: CanonicalExchangeKey
  private lateinit var recurringExchangeId: String

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(kingdomDataServicesProvider, inProcessKingdom)
  }

  @get:Rule val dataProviderFolder = TemporaryFolder()
  @get:Rule val modelProviderFolder = TemporaryFolder()
  @get:Rule val sharedFolder = TemporaryFolder()

  private data class ProviderContext(
    val key: ResourceKey,
    val privateStoragePath: Path,
    val scope: CoroutineScope,
  )

  private fun makeExchangesServiceClient(principal: String): ExchangesCoroutineStub {
    return ExchangesCoroutineStub(inProcessKingdom.publicApiChannel).withPrincipalName(principal)
  }

  private fun makeExchangeStepsServiceClient(principal: String): ExchangeStepsCoroutineStub {
    return ExchangeStepsCoroutineStub(inProcessKingdom.publicApiChannel)
      .withPrincipalName(principal)
  }

  private suspend fun getSteps(): List<ExchangeStep> {
    return exchangeStepsClient
      .listExchangeSteps(
        listExchangeStepsRequest {
          parent = exchangeKey.toName()
          pageSize = 50
          filter = filter { exchangeDates += EXCHANGE_DATE.toProtoDate() }
        }
      )
      .exchangeStepsList
      .sortedBy { step -> step.stepIndex }
  }

  private fun logStepStates(steps: Iterable<ExchangeStep>) {
    val stepsList = workflow.stepsList
    val message = StringBuilder("ExchangeStep states:")
    for ((state, stepsForState) in steps.groupBy { it.state }) {
      val stepsString =
        stepsForState
          .sortedBy { it.stepIndex }
          .joinToString(", ") { "${stepsList[it.stepIndex].stepId}#${it.stepIndex}" }
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
    val request = getExchangeRequest { name = exchangeKey.toName() }

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
    exchangeDateKey: ExchangeDateKey,
  ): ExchangeWorkflowDaemonForTest {
    return ExchangeWorkflowDaemonForTest(
      v2alphaChannel = inProcessKingdom.publicApiChannel,
      provider = owner.key,
      exchangeDateKey = exchangeDateKey,
      privateDirectory = owner.privateStoragePath,
    )
  }

  /**
   * specialSharedStorageSelector is created as a PrivateStorageSelector since the data is
   * pre-signed.
   */
  private fun makeSpecialSharedStorageSelector(): PrivateStorageSelector {
    val specialSharedStorageFactories:
      Map<StorageDetails.PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
      mapOf(StorageDetails.PlatformCase.FILE to ::FileSystemStorageFactory)
    val specialSharedStorageDetails = storageDetails {
      file = StorageDetailsKt.fileStorage { path = sharedFolder.root.toPath().toString() }
      visibility = StorageDetails.Visibility.PRIVATE
    }
    val specialSharedStorageInfo =
      StorageDetailsProvider(
        TestMutableSecretMap(
          mutableMapOf(recurringExchangeId to specialSharedStorageDetails.toByteString())
        )
      )
    return PrivateStorageSelector(specialSharedStorageFactories, specialSharedStorageInfo)
  }

  private suspend fun PrivateStorageSelector.writeSharedBlob(
    blobKey: String,
    contents: ByteString,
    exchangeDateKey: ExchangeDateKey,
  ) {
    getStorageClient(exchangeDateKey).writeBlob(blobKey, flowOf(contents))
  }

  private suspend fun PrivateStorageSelector.readSharedBlob(
    blobKey: String,
    exchangeDateKey: ExchangeDateKey,
  ): ByteString? {
    return getStorageClient(exchangeDateKey).getBlob(blobKey)?.toByteString()
  }

  @Before
  fun setup() = runBlocking {
    val keys =
      resourceSetup.createResourcesForWorkflow(
        SCHEDULE,
        workflow,
        EXCHANGE_DATE.toProtoDate(),
        EntityContent(
          displayName = "edp1",
          encryptionPublicKey =
            encryptionPublicKey { data = ByteString.copyFromUtf8("testMCPublicKey") },
          signingKey =
            SigningKeyHandle(TestCertificateManager.CERTIFICATE, TestCertificateManager.PRIVATE_KEY),
        ),
      )
    exchangesClient = makeExchangesServiceClient(keys.modelProviderKey.toName())
    exchangeStepsClient = makeExchangeStepsServiceClient(keys.modelProviderKey.toName())
    recurringExchangeId = keys.recurringExchangeKey.recurringExchangeId
    exchangeKey =
      CanonicalExchangeKey(
        recurringExchangeId = recurringExchangeId,
        exchangeId = EXCHANGE_DATE.toString(),
      )
    dataProviderContext =
      ProviderContext(
        keys.dataProviderKey,
        dataProviderFolder.root.toPath(),
        createScope("EDP_SCOPE"),
      )
    modelProviderContext =
      ProviderContext(
        keys.modelProviderKey,
        modelProviderFolder.root.toPath(),
        createScope("MP_SCOPE"),
      )
  }

  @Test
  fun runTest() = runBlockingTest {
    val exchangeDateKey = ExchangeDateKey(recurringExchangeId, EXCHANGE_DATE)
    val dataProviderDaemon = makeDaemon(dataProviderContext, exchangeDateKey)
    val modelProviderDaemon = makeDaemon(modelProviderContext, exchangeDateKey)

    logger.info("Shared Folder path: ${sharedFolder.root.absolutePath}")

    val dataProviderRootStorageClient: StorageClient by lazy {
      FileSystemStorageClient(dataProviderContext.privateStoragePath.toFile())
    }

    val modelProviderRootStorageClient: StorageClient by lazy {
      FileSystemStorageClient(modelProviderContext.privateStoragePath.toFile())
    }

    val tinkKeyUri = "fake-tink-key-uri"
    val modelProviderDefaults by lazy {
      DaemonStorageClientDefaults(
        modelProviderRootStorageClient,
        tinkKeyUri,
        FakeTinkKeyStorageProvider(),
      )
    }
    val dataProviderDefaults by lazy {
      DaemonStorageClientDefaults(
        dataProviderRootStorageClient,
        tinkKeyUri,
        FakeTinkKeyStorageProvider(),
      )
    }

    val modelProviderAddResource = ConfigureResource(modelProviderDefaults)
    val dataProviderAddResource = ConfigureResource(dataProviderDefaults)

    val workflowWithExchangeIdentifiers =
      workflow.copy {
        exchangeIdentifiers =
          exchangeIdentifiers.copy {
            dataProvider = dataProviderContext.key.toName()
            modelProvider = modelProviderContext.key.toName()
          }
      }
    modelProviderAddResource.addWorkflow(
      workflowWithExchangeIdentifiers.toByteString(),
      recurringExchangeId,
    )
    dataProviderAddResource.addWorkflow(
      workflowWithExchangeIdentifiers.toByteString(),
      recurringExchangeId,
    )

    modelProviderAddResource.addRootCertificates(
      dataProviderContext.key.toName(),
      TestCertificateManager.CERTIFICATE,
    )
    dataProviderAddResource.addRootCertificates(
      modelProviderContext.key.toName(),
      TestCertificateManager.CERTIFICATE,
    )

    val modelProviderPrivateStorageDetails = storageDetails {
      file =
        StorageDetailsKt.fileStorage { path = modelProviderContext.privateStoragePath.toString() }
      visibility = StorageDetails.Visibility.PRIVATE
    }
    modelProviderAddResource.addPrivateStorageInfo(
      recurringExchangeId,
      modelProviderPrivateStorageDetails,
    )
    val dataProviderPrivateStorageDetails = storageDetails {
      file =
        StorageDetailsKt.fileStorage { path = dataProviderContext.privateStoragePath.toString() }
      visibility = StorageDetails.Visibility.PRIVATE
    }
    dataProviderAddResource.addPrivateStorageInfo(
      recurringExchangeId,
      dataProviderPrivateStorageDetails,
    )

    val sharedStorageDetails = storageDetails {
      file = StorageDetailsKt.fileStorage { path = sharedFolder.root.toPath().toString() }
      visibility = StorageDetails.Visibility.SHARED
    }
    modelProviderAddResource.addSharedStorageInfo(recurringExchangeId, sharedStorageDetails)
    dataProviderAddResource.addSharedStorageInfo(recurringExchangeId, sharedStorageDetails)

    val privateStorageFactories:
      Map<StorageDetails.PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
      mapOf(StorageDetails.PlatformCase.FILE to ::FileSystemStorageFactory)

    for ((blobKey, value) in initialDataProviderInputs) {
      dataProviderAddResource.provideWorkflowInput(
        recurringExchangeId,
        EXCHANGE_DATE,
        privateStorageFactories,
        blobKey,
        value,
      )
    }

    for ((blobKey, value) in initialModelProviderInputs) {
      modelProviderAddResource.provideWorkflowInput(
        recurringExchangeId,
        EXCHANGE_DATE,
        privateStorageFactories,
        blobKey,
        value,
      )
    }

    val specialSharedStorageSelector: PrivateStorageSelector = makeSpecialSharedStorageSelector()
    for ((blobKey, value) in initialSharedInputs) {
      specialSharedStorageSelector.writeSharedBlob(blobKey, value, exchangeDateKey)
    }

    val edpJob = dataProviderContext.scope.launch { dataProviderDaemon.runSuspending() }
    val mpJob = modelProviderContext.scope.launch { modelProviderDaemon.runSuspending() }

    while (!isDone()) {
      delay(500)
    }

    edpJob.cancelAndJoin()
    mpJob.cancelAndJoin()

    validateFinalState(dataProviderDaemon, modelProviderDaemon)

    validateSharedStorageState(specialSharedStorageSelector, exchangeDateKey)

    val steps = getSteps()
    assertThat(steps.size == workflow.stepsCount)

    for (step in steps) {
      assertWithMessage("Step ${step.stepIndex}")
        .that(step.state)
        .isEqualTo(ExchangeStep.State.SUCCEEDED)
    }
  }

  companion object {
    private val logger by loggerFor()
    private val typeRegistry =
      TypeRegistry.newBuilder().add(Shared.Parameters.getDescriptor()).build()
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val REDIRECT_URI = "https://localhost:2048"

    // TODO(@yunyeng): Think about the tests that start running around midnight.
    val EXCHANGE_DATE: LocalDate = LocalDate.now()

    fun readExchangeWorkflowTextProto(exchangeWorkflowResourcePath: String): ExchangeWorkflow {
      return checkNotNull(this::class.java.getResource(exchangeWorkflowResourcePath))
        .openStream()
        .use { input -> parseTextProto(input.bufferedReader(), exchangeWorkflow {}, typeRegistry) }
        .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
    }
  }
}
