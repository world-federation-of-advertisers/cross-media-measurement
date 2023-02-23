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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.testing.GrpcCleanupRule
import io.opentelemetry.api.GlobalOpenTelemetry
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.testing.withMetadataDuchyIdentities
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.herald.ContinuationTokenManager
import org.wfanet.measurement.duchy.daemon.herald.Herald
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.service.api.v2alpha.RequisitionFulfillmentService
import org.wfanet.measurement.duchy.service.internal.computationcontrol.AsyncComputationControlService
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.duchy.service.system.v1alpha.ComputationControlService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub as SystemComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub as SystemRequisitionsCoroutineStub

/**
 * TestRule that starts and stops all Duchy gRPC services and daemons.
 *
 * @param externalDuchyId the external ID of this duchy
 * @param kingdomSystemApiChannel a gRPC channel to the Kingdom
 * @param duchyDependenciesProvider provides the backends and other inputs required to start a Duchy
 * @param verboseGrpcLogging whether to do verboseGrpcLogging
 */
class InProcessDuchy(
  val externalDuchyId: String,
  val kingdomSystemApiChannel: Channel,
  duchyDependenciesProvider: () -> DuchyDependencies,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  val verboseGrpcLogging: Boolean = true,
  daemonContext: CoroutineContext = Dispatchers.Default,
) : TestRule {
  data class DuchyDependencies(
    val computationsDatabase: ComputationsDatabase,
    val storageClient: StorageClient,
    val continuationTokensService: ContinuationTokensCoroutineImplBase,
  )

  private val daemonScope = CoroutineScope(daemonContext)
  private lateinit var heraldJob: Job
  private lateinit var llv2MillJob: Job

  private val duchyDependencies by lazy { duchyDependenciesProvider() }

  private val systemComputationsClient by lazy {
    SystemComputationsCoroutineStub(kingdomSystemApiChannel).withDuchyId(externalDuchyId)
  }
  private val systemComputationLogEntriesClient by lazy {
    SystemComputationLogEntriesCoroutineStub(kingdomSystemApiChannel).withDuchyId(externalDuchyId)
  }
  private val systemComputationParticipantsClient by lazy {
    SystemComputationParticipantsCoroutineStub(kingdomSystemApiChannel).withDuchyId(externalDuchyId)
  }
  private val systemRequisitionsClient by lazy {
    SystemRequisitionsCoroutineStub(kingdomSystemApiChannel).withDuchyId(externalDuchyId)
  }
  private val computationsClient by lazy { ComputationsCoroutineStub(computationsServer.channel) }
  private val computationStatsClient by lazy {
    ComputationStatsCoroutineStub(computationsServer.channel)
  }
  private val asyncComputationControlClient by lazy {
    AsyncComputationControlCoroutineStub(asyncComputationControlServer.channel)
  }
  private val continuationTokensClient by lazy {
    ContinuationTokensCoroutineStub(computationsServer.channel)
  }

  private val computationsServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        ComputationsService(
          duchyDependencies.computationsDatabase,
          systemComputationLogEntriesClient,
          ComputationStore(duchyDependencies.storageClient),
          RequisitionStore(duchyDependencies.storageClient),
          externalDuchyId,
        )
      )
      addService(ComputationStatsService(duchyDependencies.computationsDatabase))
      addService(duchyDependencies.continuationTokensService)
    }
  private val requisitionFulfillmentServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        RequisitionFulfillmentService(
            systemRequisitionsClient,
            computationsClient,
            RequisitionStore(duchyDependencies.storageClient)
          )
          .withMetadataPrincipalIdentities()
      )
    }
  private val asyncComputationControlServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(AsyncComputationControlService(computationsClient))
    }
  private val computationControlServer =
    GrpcTestServerRule(
      computationControlChannelName(externalDuchyId),
      logAllRequests = verboseGrpcLogging
    ) {
      addService(
        ComputationControlService(asyncComputationControlClient, duchyDependencies.storageClient)
          .withMetadataDuchyIdentities()
      )
    }

  private val computationDataClients by lazy {
    ComputationDataClients(
      ComputationsCoroutineStub(computationsServer.channel),
      duchyDependencies.storageClient
    )
  }

  fun startHerald() {
    heraldJob =
      daemonScope.launch(
        CoroutineName("$externalDuchyId Herald") +
          CoroutineExceptionHandler { _, e ->
            logger.log(Level.SEVERE, e) { "Error in $externalDuchyId Herald" }
          }
      ) {
        val protocolsSetupConfig =
          if (externalDuchyId == LLV2_AGGREGATOR_NAME) {
            AGGREGATOR_PROTOCOLS_SETUP_CONFIG
          } else {
            NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG
          }
        val herald =
          Herald(
            heraldId = "$externalDuchyId Herald",
            duchyId = externalDuchyId,
            internalComputationsClient = computationsClient,
            systemComputationsClient = systemComputationsClient,
            systemComputationParticipantClient = systemComputationParticipantsClient,
            continuationTokenManager = ContinuationTokenManager(continuationTokensClient),
            protocolsSetupConfig = protocolsSetupConfig,
            clock = Clock.systemUTC(),
          )
        herald.continuallySyncStatuses()
      }
  }

  suspend fun stopHerald() {
    if (this::heraldJob.isInitialized) {
      heraldJob.cancel("Stopping Herald")
      heraldJob.join()
    }
  }

  fun startLiquidLegionsV2mill(duchyCertMap: Map<String, String>) {
    val consentSignal509Cert =
      readCertificate(loadTestCertDerFile("${externalDuchyId}_cs_cert.der"))
    val signingPrivateKey =
      readPrivateKey(
        loadTestCertDerFile("${externalDuchyId}_cs_private.der"),
        consentSignal509Cert.publicKey.algorithm
      )
    llv2MillJob =
      daemonScope.launch(CoroutineName("$externalDuchyId LLv2 Mill")) {
        val signingKey = SigningKeyHandle(consentSignal509Cert, signingPrivateKey)
        val workerStubs =
          ALL_DUCHY_NAMES.minus(externalDuchyId).associateWith {
            val channel = computationControlChannel(it)
            SystemComputationControlCoroutineStub(channel).withDuchyId(externalDuchyId)
          }
        val liquidLegionsV2mill =
          LiquidLegionsV2Mill(
            millId = "$externalDuchyId liquidLegionsV2 Mill",
            duchyId = externalDuchyId,
            signingKey = signingKey,
            consentSignalCert = Certificate(duchyCertMap[externalDuchyId]!!, consentSignal509Cert),
            trustedCertificates = trustedCertificates,
            dataClients = computationDataClients,
            systemComputationParticipantsClient = systemComputationParticipantsClient,
            systemComputationsClient = systemComputationsClient,
            systemComputationLogEntriesClient = systemComputationLogEntriesClient,
            computationStatsClient = computationStatsClient,
            throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1)),
            workerStubs = workerStubs,
            cryptoWorker = JniLiquidLegionsV2Encryption(),
            workLockDuration = Duration.ofSeconds(1),
            openTelemetry = GlobalOpenTelemetry.get()
          )
        liquidLegionsV2mill.continuallyProcessComputationQueue()
      }
  }

  suspend fun stopLiquidLegionsV2Mill() {
    if (this::llv2MillJob.isInitialized) {
      llv2MillJob.cancel("Stopping LLv2 Mill")
      llv2MillJob.join()
    }
  }

  private val channelCloserRule = GrpcCleanupRule()

  private fun computationControlChannelName(duchyName: String) =
    "duchy-computation-control-$duchyName"

  private fun computationControlChannel(duchyName: String): Channel {
    val channel = InProcessChannelBuilder.forName(computationControlChannelName(duchyName)).build()
    return channelCloserRule.register(channel).withVerboseLogging(verboseGrpcLogging)
  }

  /** Provides a gRPC channel to the duchy's public API. */
  val publicApiChannel: Channel
    get() = requisitionFulfillmentServer.channel

  override fun apply(statement: Statement, description: Description) =
    object : Statement() {
      override fun evaluate() {
        val combinedRule =
          chainRulesSequentially(
            computationsServer,
            requisitionFulfillmentServer,
            asyncComputationControlServer,
            computationControlServer,
            channelCloserRule
          )
        combinedRule.apply(statement, description).evaluate()
        daemonScope.cancel()
      }
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
