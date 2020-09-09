package org.wfanet.measurement.integration

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
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.launchAsAutoCloseable
import org.wfanet.measurement.db.duchy.computation.ComputationsBlobDb
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.db.duchy.computation.SingleProtocolDatabase
import org.wfanet.measurement.db.duchy.metricvalue.MetricValueDatabase
import org.wfanet.measurement.duchy.herald.LiquidLegionsHerald
import org.wfanet.measurement.duchy.mill.CryptoKeySet
import org.wfanet.measurement.duchy.mill.LiquidLegionsCryptoWorkerImpl
import org.wfanet.measurement.duchy.mill.LiquidLegionsMill
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.service.common.withVerboseLogging
import org.wfanet.measurement.service.internal.duchy.computation.control.LiquidLegionsComputationControlServiceImpl
import org.wfanet.measurement.service.internal.duchy.computation.storage.ComputationStorageServiceImpl
import org.wfanet.measurement.service.internal.duchy.metricvalues.MetricValuesService
import org.wfanet.measurement.service.testing.GrpcTestServerRule
import org.wfanet.measurement.service.v1alpha.publisherdata.PublisherDataService
import org.wfanet.measurement.storage.StorageClient

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
  duchyId: String,
  otherDuchyIds: List<String>,
  kingdomChannel: Channel,
  duchyDependenciesProvider: () -> DuchyDependencies
) : TestRule {
  data class DuchyDependencies(
    val singleProtocolDatabase: SingleProtocolDatabase,
    val blobDb: ComputationsBlobDb<LiquidLegionsSketchAggregationStage>,
    val metricValueDatabase: MetricValueDatabase,
    val storageClient: StorageClient,
    val cryptoKeySet: CryptoKeySet
  )

  private val duchyDependencies by lazy { duchyDependenciesProvider() }

  private val kingdomGlobalComputationsStub by lazy {
    GlobalComputationsCoroutineStub(kingdomChannel).withDuchyId(duchyId)
  }

  private val storageServer = GrpcTestServerRule(logAllRequests = true) {
    addService(ComputationStorageServiceImpl(duchyDependencies.singleProtocolDatabase))
  }

  private val metricValuesServer = GrpcTestServerRule(logAllRequests = true) {
    addService(
      MetricValuesService(duchyDependencies.metricValueDatabase, duchyDependencies.storageClient)
    )
  }

  private val computationStorageServiceStub by lazy {
    ComputationStorageServiceCoroutineStub(storageServer.channel)
  }

  private val heraldRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000))
      val herald = LiquidLegionsHerald(
        otherDuchyIds,
        computationStorageServiceStub,
        kingdomGlobalComputationsStub
      )

      herald.continuallySyncStatuses(throttler)
    }
  }

  private val computationStorageClients by lazy {
    LiquidLegionsSketchAggregationComputationStorageClients(
      ComputationStorageServiceCoroutineStub(storageServer.channel),
      duchyDependencies.blobDb,
      otherDuchyIds
    )
  }

  private val computationControlServer =
    GrpcTestServerRule(computationControlChannelName(duchyId), logAllRequests = true) {
      addService(
        LiquidLegionsComputationControlServiceImpl(computationStorageClients).withDuchyIdentities()
      )
    }

  private val channelCloserRule = GrpcCleanupRule()

  private fun computationControlChannelName(duchyId: String) = "duchy-computation-control-$duchyId"

  private fun computationControlChannel(duchyId: String): Channel {
    val channel =
      InProcessChannelBuilder
        .forName(computationControlChannelName(duchyId))
        .build()
    return channelCloserRule.register(channel).withVerboseLogging()
  }

  private val millRule = CloseableResource {
    GlobalScope.launchAsAutoCloseable {
      val workerStubs = otherDuchyIds.map { otherDuchyId ->
        val channel = computationControlChannel(otherDuchyId)
        val stub = ComputationControlServiceCoroutineStub(channel).withDuchyId(duchyId)
        otherDuchyId to stub
      }.toMap()

      val mill = LiquidLegionsMill(
        millId = "$duchyId mill",
        storageClients = computationStorageClients,
        metricValuesClient = MetricValuesCoroutineStub(metricValuesServer.channel),
        globalComputationsClient = kingdomGlobalComputationsStub,
        workerStubs = workerStubs,
        cryptoKeySet = duchyDependencies.cryptoKeySet,
        cryptoWorker = LiquidLegionsCryptoWorkerImpl(),
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(200)),
        chunkSize = 20
      )

      mill.continuallyProcessComputationQueue()
    }
  }

  private val publisherDataChannelName = "duchy-publisher-data-$duchyId"

  private val publisherDataServer =
    GrpcTestServerRule(publisherDataChannelName, logAllRequests = true) {
      addService(
        PublisherDataService(
          MetricValuesCoroutineStub(metricValuesServer.channel),
          RequisitionCoroutineStub(kingdomChannel).withDuchyId(duchyId),
          DataProviderRegistrationCoroutineStub(kingdomChannel).withDuchyId(duchyId)
        )
      )
    }

  fun newPublisherDataProviderStub(): PublisherDataCoroutineStub {
    val channel = InProcessChannelBuilder.forName(publisherDataChannelName).build()
    channelCloserRule.register(channel)
    return PublisherDataCoroutineStub(channel.withVerboseLogging())
  }

  override fun apply(statement: Statement, description: Description): Statement {
    val combinedRule = chainRulesSequentially(
      storageServer,
      metricValuesServer,
      heraldRule,
      millRule,
      computationControlServer,
      publisherDataServer,
      channelCloserRule
    )
    return combinedRule.apply(statement, description)
  }
}
