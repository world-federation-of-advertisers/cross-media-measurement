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

package org.wfanet.measurement.duchy.mill

import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.addChannelShutdownHooks
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.crypto.DuchyPublicKeys
import org.wfanet.measurement.crypto.ElGamalKeyPair
import org.wfanet.measurement.db.duchy.computation.ComputationsBlobDb
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import picocli.CommandLine

abstract class LiquidLegionsMillDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: MillFlags
    private set

  private val duchyPublicKeys: DuchyPublicKeys by lazy {
    DuchyPublicKeys.fromFlags(flags.duchyPublicKeys)
  }

  protected fun run(
    computationStore: ComputationsBlobDb<LiquidLegionsSketchAggregationStage>
  ) {
    val duchyName = flags.duchy.duchyName
    val latestDuchyPublicKeys = duchyPublicKeys.latest
    require(latestDuchyPublicKeys.containsKey(duchyName)) {
      "Public key not specified for Duchy $duchyName"
    }

    val otherDuchyNames = latestDuchyPublicKeys.keys.filter { it != duchyName }
    val storageClients = LiquidLegionsSketchAggregationComputationStorageClients(
      ComputationStorageServiceCoroutineStub(buildChannel(flags.computationStorageServiceTarget))
        .withDuchyId(duchyName),
      computationStore,
      otherDuchyNames
    )

    val computationControlClientMap = flags.computationControlServiceTargets.mapValues {
      val otherDuchyChannel = buildChannel(it.value)
      ComputationControlServiceCoroutineStub(otherDuchyChannel).withDuchyId(duchyName)
    }

    val globalComputationsClient =
      GlobalComputationsCoroutineStub(buildChannel(flags.globalComputationsServiceTarget))
        .withDuchyId(duchyName)
    val metricValuesClient =
      MetricValuesCoroutineStub(buildChannel(flags.metricValuesServiceTarget))
        .withDuchyId(duchyName)

    val mill = LiquidLegionsMill(
      millId = flags.millId,
      storageClients = storageClients,
      metricValuesClient = metricValuesClient,
      globalComputationsClient = globalComputationsClient,
      workerStubs = computationControlClientMap,
      cryptoKeySet = newCryptoKeySet(),
      cryptoWorker = LiquidLegionsCryptoWorkerImpl(),
      throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval),
      chunkSize = flags.chunkSize,
      liquidLegionsConfig = LiquidLegionsMill.LiquidLegionsConfig(
        flags.liquidLegionsDecayRate, flags.liquidLegionsSize
      )
    )

    runBlocking { mill.continuallyProcessComputationQueue() }
  }

  private fun newCryptoKeySet(): CryptoKeySet {
    val latestDuchyPublicKeys = duchyPublicKeys.latest
    val keyPair =
      ElGamalKeyPair(
        latestDuchyPublicKeys.getValue(flags.duchy.duchyName),
        ByteString.copyFrom(BaseEncoding.base16().decode(flags.duchySecretKey))
      )
    return CryptoKeySet(
      ownPublicAndPrivateKeys = keyPair.toProtoMessage(),
      otherDuchyPublicKeys = latestDuchyPublicKeys.mapValues { it.value.toProtoMessage() },
      clientPublicKey = latestDuchyPublicKeys.combinedPublicKey.toProtoMessage(),
      curveId = latestDuchyPublicKeys.combinedPublicKey.ellipticCurveId
    )
  }

  private fun buildChannel(target: String): ManagedChannel {
    return ManagedChannelBuilder.forTarget(target).build().apply { addChannelShutdownHooks(this) }
  }

  private fun addChannelShutdownHooks(vararg channels: ManagedChannel) {
    addChannelShutdownHooks(Runtime.getRuntime(), flags.channelShutdownTimeout, *channels)
  }
}
