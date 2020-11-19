// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill

import io.grpc.ManagedChannel
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.ElGamalKeyPair
import org.wfanet.measurement.common.crypto.JniProtocolEncryption
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.hexAsByteString
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.LiquidLegionsMill
import org.wfanet.measurement.duchy.db.computation.LiquidLegionsSketchAggregationComputationDataClients
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import picocli.CommandLine

abstract class LiquidLegionsMillDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: MillFlags
    private set

  private val duchyPublicKeys: DuchyPublicKeys by lazy {
    DuchyPublicKeys.fromFlags(flags.duchyPublicKeys)
  }

  protected fun run(storageClient: StorageClient) {
    val duchyName = flags.duchy.duchyName
    val latestDuchyPublicKeys = duchyPublicKeys.latest
    require(latestDuchyPublicKeys.containsKey(duchyName)) {
      "Public key not specified for Duchy $duchyName"
    }

    val otherDuchyNames = latestDuchyPublicKeys.keys.filter { it != duchyName }
    val computationsServiceChannel = buildChannel(flags.computationsServiceTarget)
    val dataClients = LiquidLegionsSketchAggregationComputationDataClients(
      ComputationsCoroutineStub(computationsServiceChannel)
        .withDuchyId(duchyName),
      storageClient,
      otherDuchyNames
    )

    val computationControlClientMap = flags.computationControlServiceTargets
      .filterKeys { it != duchyName }
      .mapValues {
        val otherDuchyChannel = buildChannel(it.value)
        ComputationControlCoroutineStub(otherDuchyChannel).withDuchyId(duchyName)
      }

    val globalComputationsClient =
      GlobalComputationsCoroutineStub(buildChannel(flags.globalComputationsServiceTarget))
        .withDuchyId(duchyName)
    val computationStatsClient =
      ComputationStatsCoroutineStub(computationsServiceChannel)
    val metricValuesClient =
      MetricValuesCoroutineStub(buildChannel(flags.metricValuesServiceTarget))
        .withDuchyId(duchyName)

    val mill = LiquidLegionsMill(
      millId = flags.millId,
      dataClients = dataClients,
      metricValuesClient = metricValuesClient,
      globalComputationsClient = globalComputationsClient,
      computationStatsClient = computationStatsClient,
      workerStubs = computationControlClientMap,
      cryptoKeySet = newCryptoKeySet(),
      cryptoWorker = JniProtocolEncryption(),
      throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval),
      chunkSize = flags.chunkSize,
      liquidLegionsConfig = LiquidLegionsMill.LiquidLegionsConfig(
        flags.liquidLegionsDecayRate,
        flags.liquidLegionsSize,
        flags.sketchMaxFrequency
      )
    )

    runBlocking { mill.continuallyProcessComputationQueue() }
  }

  private fun newCryptoKeySet(): CryptoKeySet {
    val latestDuchyPublicKeys = duchyPublicKeys.latest
    return CryptoKeySet(
      ownPublicAndPrivateKeys = ElGamalKeyPair.newBuilder().apply {
        publicKey = latestDuchyPublicKeys.getValue(flags.duchy.duchyName)
        secretKey = flags.duchySecretKey.hexAsByteString()
      }.build(),
      otherDuchyPublicKeys = latestDuchyPublicKeys.mapValues { it.value },
      clientPublicKey = latestDuchyPublicKeys.combinedPublicKey,
      curveId = latestDuchyPublicKeys.curveId
    )
  }

  private fun buildChannel(target: String): ManagedChannel =
    buildChannel(target, flags.channelShutdownTimeout)
}
