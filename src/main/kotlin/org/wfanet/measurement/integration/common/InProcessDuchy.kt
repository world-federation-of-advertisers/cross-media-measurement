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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.testing.GrpcCleanupRule
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
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.testing.withMetadataDuchyIdentities
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.deploy.common.service.DuchyDataServices
import org.wfanet.measurement.duchy.herald.ContinuationTokenManager
import org.wfanet.measurement.duchy.herald.Herald
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.ReachFrequencyLiquidLegionsV2Mill
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.ReachOnlyLiquidLegionsV2Mill
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.JniReachOnlyLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.mill.shareshuffle.HonestMajorityShareShuffleMill
import org.wfanet.measurement.duchy.mill.shareshuffle.crypto.JniHonestMajorityShareShuffleCryptor
import org.wfanet.measurement.duchy.service.api.v2alpha.RequisitionFulfillmentService
import org.wfanet.measurement.duchy.service.internal.computationcontrol.AsyncComputationControlService
import org.wfanet.measurement.duchy.service.system.v1alpha.ComputationControlService
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.duchy.storage.TinkKeyStore
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
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
 * @param duchyDependenciesRule provides the data services and storage client
 * @param verboseGrpcLogging whether to do verboseGrpcLogging
 */
class InProcessDuchy(
  val externalDuchyId: String,
  val kingdomSystemApiChannel: Channel,
  val kingdomPublicApiChannel: Channel,
  val duchyDependenciesRule:
    ProviderRule<(String, SystemComputationLogEntriesCoroutineStub) -> DuchyDependencies>,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  val verboseGrpcLogging: Boolean = true,
  daemonContext: CoroutineContext = Dispatchers.Default,
) : TestRule {
  data class DuchyDependencies(
    val duchyDataServices: DuchyDataServices,
    val storageClient: StorageClient,
  )

  private val daemonScope = CoroutineScope(daemonContext)
  private lateinit var heraldJob: Job
  private lateinit var millJob: Job

  private val duchyDependencies by lazy {
    duchyDependenciesRule.value(externalDuchyId, systemComputationLogEntriesClient)
  }

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
  private val certificateStub: CertificatesGrpcKt.CertificatesCoroutineStub by lazy {
    CertificatesGrpcKt.CertificatesCoroutineStub(kingdomPublicApiChannel)
      .withPrincipalName(DuchyKey(externalDuchyId).toName())
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

  // TODO(@renjiez): Use real PrivateKeyStore when enabling HMSS.
  private val privateKeyStore by lazy {
    val keyUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val privateKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    val aead = privateKeyHandle.getPrimitive(Aead::class.java)
    val fakeKmsClient = FakeKmsClient().also { it.setAead(keyUri, aead) }
    TinkKeyStorageProvider(fakeKmsClient)
      .makeKmsPrivateKeyStore(TinkKeyStore(InMemoryStorageClient()), keyUri)
  }

  private val serviceDispatcher = Dispatchers.Default

  private val computationsServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(duchyDependencies.duchyDataServices.computationsService)
      addService(duchyDependencies.duchyDataServices.computationStatsService)
      addService(duchyDependencies.duchyDataServices.continuationTokensService)
    }
  private val requisitionFulfillmentServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        RequisitionFulfillmentService(
            externalDuchyId,
            systemRequisitionsClient,
            computationsClient,
            RequisitionStore(duchyDependencies.storageClient),
            serviceDispatcher,
          )
          .withMetadataPrincipalIdentities()
      )
    }
  private val asyncComputationControlServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        AsyncComputationControlService(
          computationsClient,
          maxAdvanceAttempts = Int.MAX_VALUE,
          coroutineContext = serviceDispatcher,
        )
      )
    }

  private val computationControlServer =
    GrpcTestServerRule(
      computationControlChannelName(externalDuchyId),
      logAllRequests = verboseGrpcLogging,
    ) {
      addService(
        ComputationControlService(
            externalDuchyId,
            computationsClient,
            asyncComputationControlClient,
            duchyDependencies.storageClient,
            serviceDispatcher,
          )
          .withMetadataDuchyIdentities()
      )
    }

  private val computationDataClients by lazy {
    ComputationDataClients(
      ComputationsCoroutineStub(computationsServer.channel),
      duchyDependencies.storageClient,
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
          when (externalDuchyId) {
            AGGREGATOR_NAME -> AGGREGATOR_PROTOCOLS_SETUP_CONFIG
            WORKER1_NAME -> WORKER1_PROTOCOLS_SETUP_CONFIG
            WORKER2_NAME -> WORKER2_PROTOCOLS_SETUP_CONFIG
            else -> error("Protocol setup config for duchy $externalDuchyId not found.")
          }
        val herald =
          Herald(
            heraldId = "$externalDuchyId Herald",
            duchyId = externalDuchyId,
            internalComputationsClient = computationsClient,
            systemComputationsClient = systemComputationsClient,
            systemComputationParticipantClient = systemComputationParticipantsClient,
            privateKeyStore = privateKeyStore,
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

  fun startMill(duchyCertMap: Map<String, String>) {
    val consentSignal509Cert =
      readCertificate(loadTestCertDerFile("${externalDuchyId}_cs_cert.der"))
    val signingPrivateKey =
      readPrivateKey(
        loadTestCertDerFile("${externalDuchyId}_cs_private.der"),
        consentSignal509Cert.publicKey.algorithm,
      )
    millJob =
      daemonScope.launch(CoroutineName("$externalDuchyId Mill")) {
        val signingKey = SigningKeyHandle(consentSignal509Cert, signingPrivateKey)
        val workerStubs =
          ALL_DUCHY_NAMES.minus(externalDuchyId).associateWith {
            val channel = computationControlChannel(it)
            SystemComputationControlCoroutineStub(channel).withDuchyId(externalDuchyId)
          }
        val protocolsSetupConfig =
          when (externalDuchyId) {
            AGGREGATOR_NAME -> AGGREGATOR_PROTOCOLS_SETUP_CONFIG
            WORKER1_NAME -> WORKER1_PROTOCOLS_SETUP_CONFIG
            WORKER2_NAME -> WORKER2_PROTOCOLS_SETUP_CONFIG
            else -> error("Protocol setup config for duchy $externalDuchyId not found.")
          }
        val reachFrequencyLiquidLegionsV2Mill =
          ReachFrequencyLiquidLegionsV2Mill(
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
            workerStubs = workerStubs,
            cryptoWorker = JniLiquidLegionsV2Encryption(),
            workLockDuration = Duration.ofSeconds(1),
            parallelism = DUCHY_MILL_PARALLELISM,
          )
        val reachOnlyLiquidLegionsV2Mill =
          ReachOnlyLiquidLegionsV2Mill(
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
            workerStubs = workerStubs,
            cryptoWorker = JniReachOnlyLiquidLegionsV2Encryption(),
            workLockDuration = Duration.ofSeconds(1),
            parallelism = DUCHY_MILL_PARALLELISM,
          )
        val honestMajorityShareShuffleMill =
          HonestMajorityShareShuffleMill(
            millId = "$externalDuchyId honestMajorityShareShuffle Mill",
            duchyId = externalDuchyId,
            signingKey = signingKey,
            consentSignalCert = Certificate(duchyCertMap[externalDuchyId]!!, consentSignal509Cert),
            trustedCertificates = trustedCertificates,
            dataClients = computationDataClients,
            systemComputationParticipantsClient = systemComputationParticipantsClient,
            systemComputationsClient = systemComputationsClient,
            systemComputationLogEntriesClient = systemComputationLogEntriesClient,
            computationStatsClient = computationStatsClient,
            certificateClient = certificateStub,
            workerStubs = workerStubs,
            cryptoWorker = JniHonestMajorityShareShuffleCryptor(),
            protocolSetupConfig = protocolsSetupConfig.honestMajorityShareShuffle,
            workLockDuration = Duration.ofSeconds(1),
            privateKeyStore = privateKeyStore,
          )
        val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1))
        throttler.loopOnReady {
          reachFrequencyLiquidLegionsV2Mill.claimAndProcessWork()
          reachOnlyLiquidLegionsV2Mill.claimAndProcessWork()
          honestMajorityShareShuffleMill.claimAndProcessWork()
        }
      }
  }

  suspend fun stopMill() {
    if (this::millJob.isInitialized) {
      millJob.cancel("Stopping Mill")
      millJob.join()
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
            channelCloserRule,
          )
        combinedRule.apply(statement, description).evaluate()
        daemonScope.cancel()
      }
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
