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
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.crypto.DuchyPublicKeys
import org.wfanet.measurement.crypto.ElGamalKeyPair
import org.wfanet.measurement.db.duchy.computation.gcp.newLiquidLegionsSketchAggregationGcpComputationStorageClients
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt
import org.wfanet.measurement.storage.gcs.GcsFromFlags
import picocli.CommandLine

@CommandLine.Command(
  name = "mill_main ",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin millFlags: MillFlags,
  @CommandLine.Mixin gcsFlags: GcsFromFlags.Flags
) {
  val duchyName = millFlags.duchy.duchyName
  val latestDuchyPublicKeys = DuchyPublicKeys.fromFlags(millFlags.duchyPublicKeys).latest
  require(latestDuchyPublicKeys.containsKey(duchyName)) {
    "Public key not specified for Duchy $duchyName"
  }

  // TODO: Expand flags and configuration to work on other cloud environments when available.
  val googleCloudStorage = GcsFromFlags(gcsFlags)

  val storageChannel: ManagedChannel =
    ManagedChannelBuilder
      .forTarget(millFlags.computationStorageServiceTarget)
      .usePlaintext()
      .build()
  val storageClients = newLiquidLegionsSketchAggregationGcpComputationStorageClients(
    duchyName = millFlags.duchy.duchyName,
    duchyPublicKeys = latestDuchyPublicKeys,
    googleCloudStorage = googleCloudStorage.storage,
    storageBucket = googleCloudStorage.bucket,
    computationStorageServiceChannel = storageChannel
  )
  val metricValuesChannel =
    ManagedChannelBuilder.forTarget(millFlags.metricValuesServiceTarget)
      .usePlaintext()
      .build()
  val metricValuesClient =
    MetricValuesGrpcKt.MetricValuesCoroutineStub(metricValuesChannel)

  val globalComputationsChannel =
    ManagedChannelBuilder.forTarget(millFlags.globalComputationsServiceTarget)
      .usePlaintext()
      .build()
  val globalComputationsClient =
    GlobalComputationsCoroutineStub(globalComputationsChannel).withDuchyId(duchyName)

  val computationControlClientMap = millFlags.computationControlServiceTargets.mapValues {
    val otherDuchyChannel = ManagedChannelBuilder.forTarget(it.value).build()
    addChannelShutdownHooks(
      Runtime.getRuntime(),
      millFlags.channelShutdownTimeout,
      otherDuchyChannel,
      storageChannel,
      globalComputationsChannel,
      metricValuesChannel
    )
    ComputationControlServiceCoroutineStub(otherDuchyChannel).withDuchyId(duchyName)
  }

  val keyPair =
    ElGamalKeyPair(
      latestDuchyPublicKeys.getValue(duchyName),
      ByteString.copyFrom(BaseEncoding.base16().decode(millFlags.duchySecretKey))
    )
  val cryptoKeySet = CryptoKeySet(
    ownPublicAndPrivateKeys = keyPair.toProtoMessage(),
    otherDuchyPublicKeys = latestDuchyPublicKeys.mapValues { it.value.toProtoMessage() },
    clientPublicKey = latestDuchyPublicKeys.combinedPublicKey.toProtoMessage(),
    curveId = latestDuchyPublicKeys.combinedPublicKey.ellipticCurveId
  )

  val pollingThrottler = MinimumIntervalThrottler(Clock.systemUTC(), millFlags.pollingInterval)
  val cryptoWorker = LiquidLegionsCryptoWorkerImpl()
  val mill = LiquidLegionsMill(
    millId = millFlags.millId,
    storageClients = storageClients,
    metricValuesClient = metricValuesClient,
    globalComputationsClient = globalComputationsClient,
    workerStubs = computationControlClientMap,
    cryptoKeySet = cryptoKeySet,
    cryptoWorker = cryptoWorker,
    throttler = pollingThrottler,
    chunkSize = millFlags.chunkSize,
    liquidLegionsConfig = LiquidLegionsMill.LiquidLegionsConfig(
      millFlags.liquidLegionsDecayRate, millFlags.liquidLegionsSize
    )
  )

  runBlocking { mill.continuallyProcessComputationQueue() }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
