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

package org.wfanet.measurement.duchy.herald

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.addChannelShutdownHooks
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.db.duchy.computation.gcp.newLiquidLegionsSketchAggregationGcpComputationStorageClients
import org.wfanet.measurement.db.gcp.GoogleCloudStorageFromFlags
import picocli.CommandLine
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates

private class HeraldFlags {
  @set:CommandLine.Option(
    names = ["--duchy-name"],
    description = ["Name of the gRPC server for logging purposes."],
    required = true,
    defaultValue = "WorkerServer"
  )
  var duchyName: String by Delegates.notNull()
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "PT3S",
    description = ["How long to allow for the gRPC channel to shutdown."],
    required = true
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "1m",
    description = ["How long to sleep between calls to the Global Computation Service."],
    required = true
  )
  lateinit var pollingInterval: Duration
    private set

  @CommandLine.Option(
    names = ["--global-computation-service"],
    description = ["Address and port of the Global Computation Service"],
    required = true,
    defaultValue = "localhost:8080"
  )
  lateinit var globalComputationsService: String
    private set

  @set:CommandLine.Option(
    names = ["--computation-storage-service-target"],
    required = true
  )
  var computationStorageServiceTarget: String by Delegates.notNull()
    private set
}

@CommandLine.Command(
  name = "gcp_worker_server",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin heraldFlags: HeraldFlags,
  // TODO: Break dependence on google cloud storage, it is not actually used.
  @CommandLine.Mixin cloudStorageFlags: GoogleCloudStorageFromFlags.Flags
) {
  val storageChannel =
    ManagedChannelBuilder.forTarget(heraldFlags.computationStorageServiceTarget)
      .usePlaintext()
      .build()
  val cloudStorage = GoogleCloudStorageFromFlags(cloudStorageFlags)
  val storageClients = newLiquidLegionsSketchAggregationGcpComputationStorageClients(
    duchyName = heraldFlags.duchyName,
    // TODO: Pass public keys of all duchies to the computation manager
    duchyPublicKeys = mapOf(),
    googleCloudStorageOptions = cloudStorage.cloudStorageOptions,
    storageBucket = cloudStorage.bucket,
    computationStorageServiceChannel = storageChannel
  )

  val channel =
    ManagedChannelBuilder.forTarget(heraldFlags.globalComputationsService)
      .usePlaintext()
      .build()
  addChannelShutdownHooks(Runtime.getRuntime(), heraldFlags.channelShutdownTimeout, channel)
  val globalComputationsServiceClient =
    GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub(channel)

  val pollingThrottler = MinimumIntervalThrottler(Clock.systemUTC(), heraldFlags.pollingInterval)
  val herald = LiquidLegionsHerald(
    storageClients = storageClients,
    globalComputationsClient = globalComputationsServiceClient
  )
  runBlocking { herald.continuallySyncStatuses(pollingThrottler) }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
