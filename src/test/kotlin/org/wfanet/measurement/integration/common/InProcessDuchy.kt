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

import io.grpc.Channel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.testing.GrpcCleanupRule
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.GlobalScope
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.herald.Herald
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.service.internal.computation.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationcontrol.AsyncComputationControlService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.duchy.service.system.v1alpha.ComputationControlService
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemCComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub

/**
 * TestRule that starts and stops all Duchy gRPC services and daemons.
 *
 * @param duchyId the name of this duchy
 * @param otherDuchyIds the names of other duchies
 * @param kingdomChannel a gRPC channel to the Kingdom
 * @param duchyDependenciesProvider provides the backends and other inputs required to start a Duchy
 */
class InProcessDuchy(
  val verboseGrpcLogging: Boolean = true,
  duchyId: String,
  otherDuchyIds: List<String>,
  kingdomChannel: Channel,
  duchyDependenciesProvider: () -> DuchyDependencies
) : TestRule {
  data class DuchyDependencies(
    val computationsDatabase: ComputationsDatabase,
    val storageClient: StorageClient
  )

  private val duchyDependencies by lazy { duchyDependenciesProvider() }

  private val systemComputationsStub by lazy {
    SystemComputationsCoroutineStub(kingdomChannel).withDuchyId(duchyId)
  }

  private val systemComputationLogEntriesStub by lazy {
    SystemCComputationLogEntriesCoroutineStub(kingdomChannel).withDuchyId(duchyId)
  }

  private val systemComputationParticipantsStub by lazy {
    SystemComputationParticipantsCoroutineStub(kingdomChannel).withDuchyId(duchyId)
  }

  private val computationStatsStub by lazy {
    ComputationStatsCoroutineStub(computationControlChannel(duchyId))
  }

  private val duchyComputationsServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        ComputationsService(
          duchyDependencies.computationsDatabase,
          systemComputationLogEntriesStub,
          duchyId,
          Clock.systemUTC()
        )
      )
    }

  private val duchyComputationsStub by lazy {
    ComputationsCoroutineStub(duchyComputationsServer.channel)
  }

  private val heraldRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
      val protocolsSetupConfig =
        ProtocolsSetupConfig.newBuilder()
          .apply {
            liquidLegionsV2Builder.apply {
              role =
                if (duchyId == DUCHY_IDS.first()) {
                  LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR
                } else {
                  LiquidLegionsV2SetupConfig.RoleInComputation.NON_AGGREGATOR
                }
            }
          }
          .build()
      val herald =
        Herald(
          otherDuchyIds,
          duchyComputationsStub,
          systemComputationsStub,
          protocolsSetupConfig,
          mapOf() // TODO(wangyaopw): used a test PublicApiProtocolConfigs.
        )

      herald.continuallySyncStatuses(throttler)
    }
  }

  private val computationDataClients by lazy {
    ComputationDataClients(
      ComputationsCoroutineStub(duchyComputationsServer.channel),
      duchyDependencies.storageClient,
      otherDuchyIds
    )
  }

  private val asyncComputationControlServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        AsyncComputationControlService(
          ComputationsCoroutineStub(duchyComputationsServer.channel),
          ComputationProtocolStageDetails(otherDuchyIds)
        )
      )
    }

  private val computationControlServer =
    GrpcTestServerRule(
      computationControlChannelName(duchyId),
      logAllRequests = verboseGrpcLogging
    ) {
      addService(
        ComputationControlService(
            AsyncComputationControlCoroutineStub(asyncComputationControlServer.channel),
            duchyDependencies.storageClient
          )
          .withDuchyIdentities()
      )
      addService(ComputationStatsService(duchyDependencies.computationsDatabase))
    }

  private val channelCloserRule = GrpcCleanupRule()

  private fun computationControlChannelName(duchyId: String) = "duchy-computation-control-$duchyId"

  private fun computationControlChannel(duchyId: String): Channel {
    val channel = InProcessChannelBuilder.forName(computationControlChannelName(duchyId)).build()
    return channelCloserRule.register(channel).withVerboseLogging(verboseGrpcLogging)
  }

  private val liquidLegionsV2millRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val workerStubs =
        otherDuchyIds
          .map { otherDuchyId ->
            val channel = computationControlChannel(otherDuchyId)
            val stub = ComputationControlCoroutineStub(channel).withDuchyId(duchyId)
            otherDuchyId to stub
          }
          .toMap()

      val liquidLegionsV2mill =
        LiquidLegionsV2Mill(
          millId = "$duchyId liquidLegionsV2mill",
          duchyId = duchyId,
          dataClients = computationDataClients,
          systemComputationParticipantsClient = systemComputationParticipantsStub,
          systemComputationsClient = systemComputationsStub,
          systemComputationLogEntriesClient = systemComputationLogEntriesStub,
          computationStatsClient = computationStatsStub,
          workerStubs = workerStubs,
          cryptoWorker = JniLiquidLegionsV2Encryption(),
          throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
          requestChunkSizeBytes = 2_000_000
        )

      liquidLegionsV2mill.continuallyProcessComputationQueue()
    }
  }

  override fun apply(statement: Statement, description: Description): Statement {
    val combinedRule =
      chainRulesSequentially(
        duchyComputationsServer,
        heraldRule,
        liquidLegionsV2millRule,
        asyncComputationControlServer,
        computationControlServer,
        channelCloserRule
      )
    return combinedRule.apply(statement, description)
  }
}
