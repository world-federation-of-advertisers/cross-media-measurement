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
import org.wfanet.measurement.api.v1alpha.DataProviderRegistrationGrpcKt.DataProviderRegistrationCoroutineStub
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.crypto.LiquidLegionsV2NoiseConfig
import org.wfanet.measurement.common.crypto.liquidlegionsv1.JniLiquidLegionsV1Encryption
import org.wfanet.measurement.common.crypto.liquidlegionsv2.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.daemon.herald.Herald
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv1.LiquidLegionsV1Mill
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.metricvalue.MetricValueDatabase
import org.wfanet.measurement.duchy.service.api.v1alpha.PublisherDataService
import org.wfanet.measurement.duchy.service.internal.computation.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationcontrol.AsyncComputationControlService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.duchy.service.internal.metricvalues.MetricValuesService
import org.wfanet.measurement.duchy.service.system.v1alpha.ComputationControlService
import org.wfanet.measurement.duchy.toDuchyOrder
import org.wfanet.measurement.internal.duchy.AsyncComputationControlGrpcKt.AsyncComputationControlCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.system.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub as SystemRequisitionCoroutineStub

/**
 * TestRule that starts and stops all Duchy gRPC services and daemons.
 *
 * @param duchyId the name of this duchy
 * @param otherDuchyIds the names of other duchies
 * @param kingdomChannel a gRPC channel to the Kingdom
 * @param duchyDependenciesProvider provides the backends and other inputs required to start a Duchy
 *
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
    val metricValueDatabase: MetricValueDatabase,
    val storageClient: StorageClient,
    val duchyPublicKeys: DuchyPublicKeys,
    val cryptoKeySet: CryptoKeySet
  )

  private val duchyDependencies by lazy { duchyDependenciesProvider() }

  private val kingdomGlobalComputationsStub by lazy {
    GlobalComputationsCoroutineStub(kingdomChannel).withDuchyId(duchyId)
  }

  private val computationStatsStub by lazy {
    ComputationStatsCoroutineStub(computationControlChannel(duchyId))
  }

  private val storageServer = GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
    addService(
      ComputationsService(
        duchyDependencies.computationsDatabase,
        kingdomGlobalComputationsStub,
        duchyId,
        Clock.systemUTC()
      )
    )
  }

  private val metricValuesServer = GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
    addService(
      MetricValuesService(duchyDependencies.metricValueDatabase, duchyDependencies.storageClient)
    )
  }

  private val computationStorageServiceStub by lazy {
    ComputationsCoroutineStub(storageServer.channel)
  }

  private val heraldRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
      val herald = Herald(
        otherDuchyIds,
        computationStorageServiceStub,
        kingdomGlobalComputationsStub,
        duchyId,
        duchyDependencies.duchyPublicKeys.latest.toDuchyOrder()
      )

      herald.continuallySyncStatuses(throttler)
    }
  }

  private val computationDataClients by lazy {
    ComputationDataClients(
      ComputationsCoroutineStub(storageServer.channel),
      duchyDependencies.storageClient,
      otherDuchyIds
    )
  }

  private val asyncComputationControlServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      addService(
        AsyncComputationControlService(
          ComputationsCoroutineStub(storageServer.channel),
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
        ).withDuchyIdentities()
      )
      addService(
        ComputationStatsService(duchyDependencies.computationsDatabase)
      )
    }

  private val channelCloserRule = GrpcCleanupRule()

  private fun computationControlChannelName(duchyId: String) = "duchy-computation-control-$duchyId"

  private fun computationControlChannel(duchyId: String): Channel {
    val channel =
      InProcessChannelBuilder
        .forName(computationControlChannelName(duchyId))
        .build()
    return channelCloserRule.register(channel).withVerboseLogging(verboseGrpcLogging)
  }

  private val liquidLegionsV1millRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val workerStubs = otherDuchyIds.map { otherDuchyId ->
        val channel = computationControlChannel(otherDuchyId)
        val stub = ComputationControlCoroutineStub(channel).withDuchyId(duchyId)
        otherDuchyId to stub
      }.toMap()

      val liquidLegionsV1mill = LiquidLegionsV1Mill(
        millId = "$duchyId liquidLegionsV1mill",
        dataClients = computationDataClients,
        metricValuesClient = MetricValuesCoroutineStub(metricValuesServer.channel),
        globalComputationsClient = kingdomGlobalComputationsStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoKeySet = duchyDependencies.cryptoKeySet,
        cryptoWorker = JniLiquidLegionsV1Encryption(),
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        requestChunkSizeBytes = 2_000_000
      )
      liquidLegionsV1mill.continuallyProcessComputationQueue()
    }
  }

  private val liquidLegionsV2millRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val workerStubs = otherDuchyIds.map { otherDuchyId ->
        val channel = computationControlChannel(otherDuchyId)
        val stub = ComputationControlCoroutineStub(channel).withDuchyId(duchyId)
        otherDuchyId to stub
      }.toMap()

      val liquidLegionsV2mill = LiquidLegionsV2Mill(
        millId = "$duchyId liquidLegionsV2mill",
        dataClients = computationDataClients,
        metricValuesClient = MetricValuesCoroutineStub(metricValuesServer.channel),
        globalComputationsClient = kingdomGlobalComputationsStub,
        computationStatsClient = computationStatsStub,
        workerStubs = workerStubs,
        cryptoKeySet = duchyDependencies.cryptoKeySet,
        duchyOrder = duchyDependencies.duchyPublicKeys.latest.toDuchyOrder(),
        cryptoWorker = JniLiquidLegionsV2Encryption(),
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        requestChunkSizeBytes = 2_000_000,
        noiseConfig = LiquidLegionsV2NoiseConfig.getDefaultInstance()
      )

      liquidLegionsV2mill.continuallyProcessComputationQueue()
    }
  }

  private val publisherDataChannelName = "duchy-publisher-data-$duchyId"

  private val publisherDataServer =
    GrpcTestServerRule(publisherDataChannelName, logAllRequests = verboseGrpcLogging) {
      addService(
        PublisherDataService(
          MetricValuesCoroutineStub(metricValuesServer.channel),
          RequisitionCoroutineStub(kingdomChannel).withDuchyId(duchyId),
          SystemRequisitionCoroutineStub(kingdomChannel).withDuchyId(duchyId),
          DataProviderRegistrationCoroutineStub(kingdomChannel).withDuchyId(duchyId),
          duchyDependencies.duchyPublicKeys
        )
      )
    }

  fun newPublisherDataProviderStub(): PublisherDataCoroutineStub {
    val channel = InProcessChannelBuilder.forName(publisherDataChannelName).build()
    channelCloserRule.register(channel)
    return PublisherDataCoroutineStub(channel.withVerboseLogging(verboseGrpcLogging))
  }

  override fun apply(statement: Statement, description: Description): Statement {
    val combinedRule = chainRulesSequentially(
      storageServer,
      metricValuesServer,
      heraldRule,
      liquidLegionsV1millRule,
      liquidLegionsV2millRule,
      asyncComputationControlServer,
      computationControlServer,
      publisherDataServer,
      channelCloserRule
    )
    return combinedRule.apply(statement, description)
  }
}
