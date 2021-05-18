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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.liquidlegionsv2

import io.grpc.ManagedChannel
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.common.ElGamalKeyPair
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.hexAsByteString
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.daemon.mill.CryptoKeySet
import org.wfanet.measurement.duchy.daemon.mill.LiquidLegionsConfig
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsV2NoiseConfig
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import picocli.CommandLine

abstract class LiquidLegionsV2MillDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: LiquidLegionsV2MillFlags
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
    val dataClients =
      ComputationDataClients(
        ComputationsCoroutineStub(computationsServiceChannel).withDuchyId(duchyName),
        storageClient,
        otherDuchyNames
      )

    val computationControlClientMap =
      flags.computationControlServiceTargets.filterKeys { it != duchyName }.mapValues {
        val otherDuchyChannel = buildChannel(it.value)
        ComputationControlCoroutineStub(otherDuchyChannel).withDuchyId(duchyName)
      }

    val globalComputationsClient =
      GlobalComputationsCoroutineStub(buildChannel(flags.globalComputationsServiceTarget))
        .withDuchyId(duchyName)
    val computationStatsClient = ComputationStatsCoroutineStub(computationsServiceChannel)
    val metricValuesClient =
      MetricValuesCoroutineStub(buildChannel(flags.metricValuesServiceTarget))
        .withDuchyId(duchyName)

    val mill =
      LiquidLegionsV2Mill(
        millId = flags.millId,
        duchyId = flags.duchy.duchyName,
        dataClients = dataClients,
        metricValuesClient = metricValuesClient,
        globalComputationsClient = globalComputationsClient,
        computationStatsClient = computationStatsClient,
        workerStubs = computationControlClientMap,
        cryptoKeySet = newCryptoKeySet(),
        cryptoWorker = JniLiquidLegionsV2Encryption(),
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval),
        requestChunkSizeBytes = flags.requestChunkSizeBytes,
        maxFrequency = flags.sketchMaxFrequency,
        liquidLegionsConfig =
          LiquidLegionsConfig(flags.liquidLegionsDecayRate, flags.liquidLegionsSize),
        noiseConfig =
          flags.noiseConfig.reader().use {
            parseTextProto(it, LiquidLegionsV2NoiseConfig.getDefaultInstance())
          },
        aggregatorId = flags.aggregatorId
      )

    runBlocking { mill.continuallyProcessComputationQueue() }
  }

  private fun newCryptoKeySet(): CryptoKeySet {
    val latestDuchyPublicKeys = duchyPublicKeys.latest
    return CryptoKeySet(
      ownPublicAndPrivateKeys =
        ElGamalKeyPair.newBuilder()
          .apply {
            publicKey = latestDuchyPublicKeys.getValue(flags.duchy.duchyName)
            secretKey = flags.duchySecretKey.hexAsByteString()
          }
          .build(),
      allDuchyPublicKeys = latestDuchyPublicKeys.mapValues { it.value },
      clientPublicKey = latestDuchyPublicKeys.combinedPublicKey,
      curveId = latestDuchyPublicKeys.curveId
    )
  }

  private fun buildChannel(target: String): ManagedChannel =
    buildChannel(target, flags.channelShutdownTimeout)
}
