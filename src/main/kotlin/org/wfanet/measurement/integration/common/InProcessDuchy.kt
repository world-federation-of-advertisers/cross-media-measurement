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

import io.grpc.Channel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.testing.GrpcCleanupRule
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.testing.withMetadataDuchyIdentities
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.duchy.daemon.herald.Herald
import org.wfanet.measurement.duchy.daemon.mill.CONSENT_SIGNALING_PRIVATE_KEY_ID
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
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
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
  val verboseGrpcLogging: Boolean = true,
) : TestRule {
  data class DuchyDependencies(
    val computationsDatabase: ComputationsDatabase,
    val storageClient: StorageClient,
    val keyStore: KeyStore
  )

  private val backgroundScope = CoroutineScope(Dispatchers.Default)

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

  private val computationsServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        ComputationsService(
          duchyDependencies.computationsDatabase,
          systemComputationLogEntriesClient,
          externalDuchyId,
        )
      )
      addService(ComputationStatsService(duchyDependencies.computationsDatabase))
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

  private val heraldRule = CloseableResource {
    backgroundScope.launchAsAutoCloseable {
      val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
      val protocolsSetupConfig =
        if (externalDuchyId == LLV2_AGGREGATOR_NAME) {
          AGGREGATOR_PROTOCOLS_SETUP_CONFIG
        } else {
          NON_AGGREGATOR_PROTOCOLS_SETUP_CONFIG
        }
      val herald = Herald(computationsClient, systemComputationsClient, protocolsSetupConfig)
      herald.continuallySyncStatuses(throttler)
    }
  }

  fun startLiquidLegionsV2mill(duchyCertMap: Map<String, String>) {
    backgroundScope.launch {
      duchyDependencies.keyStore.storePrivateKeyDer(
        CONSENT_SIGNALING_PRIVATE_KEY_ID,
        loadTestCertDerFile("${externalDuchyId}_cs_private.der")
      )
      val workerStubs =
        ALL_DUCHY_NAMES.minus(externalDuchyId).associateWith {
          val channel = computationControlChannel(it)
          val stub = SystemComputationControlCoroutineStub(channel).withDuchyId(externalDuchyId)
          stub
        }
      val liquidLegionsV2mill =
        LiquidLegionsV2Mill(
          millId = "$externalDuchyId liquidLegionsV2 Mill",
          duchyId = externalDuchyId,
          keyStore = duchyDependencies.keyStore,
          consentSignalCert =
            Certificate(
              duchyCertMap[externalDuchyId]!!,
              readCertificate(loadTestCertDerFile("${externalDuchyId}_cs_cert.der"))
            ),
          dataClients = computationDataClients,
          systemComputationParticipantsClient = systemComputationParticipantsClient,
          systemComputationsClient = systemComputationsClient,
          systemComputationLogEntriesClient = systemComputationLogEntriesClient,
          computationStatsClient = computationStatsClient,
          throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          workerStubs = workerStubs,
          cryptoWorker = JniLiquidLegionsV2Encryption()
        )
      liquidLegionsV2mill.continuallyProcessComputationQueue()
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
            heraldRule,
            channelCloserRule
          )
        combinedRule.apply(statement, description).evaluate()
        backgroundScope.cancel()
      }
    }
}
