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

package org.wfanet.measurement.service.internal.duchy.computation.control

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.crypto.DuchyPublicKeys
import org.wfanet.measurement.db.duchy.computation.gcp.newLiquidLegionsSketchAggregationGcpComputationStorageClients
import org.wfanet.measurement.duchy.CommonDuchyFlags
import org.wfanet.measurement.storage.gcs.GcsFromFlags
import picocli.CommandLine

private const val SERVER_NAME = "LiquidLegionsComputationControlServer"

private class ComputationControlServiceFlags {
  @CommandLine.Mixin
  lateinit var server: CommonServer.Flags
    private set

  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyPublicKeys: DuchyPublicKeys.Flags
    private set

  @CommandLine.Option(
    names = ["--computation-storage-service-target"],
    required = true
  )
  lateinit var computationStorageServiceTarget: String
    private set
}

@CommandLine.Command(
  name = SERVER_NAME,
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin flags: ComputationControlServiceFlags,
  @CommandLine.Mixin gcsFlags: GcsFromFlags.Flags
) {
  val duchyName = flags.duchy.duchyName
  val latestDuchyPublicKeys = DuchyPublicKeys.fromFlags(flags.duchyPublicKeys).latest
  require(latestDuchyPublicKeys.containsKey(duchyName)) {
    "Public key not specified for Duchy $duchyName"
  }

  // TODO: Expand flags and configuration to work on other cloud environments when available.
  val googleCloudStorage = GcsFromFlags(gcsFlags)

  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forTarget(flags.computationStorageServiceTarget)
      .usePlaintext()
      .build()

  val storageClients = newLiquidLegionsSketchAggregationGcpComputationStorageClients(
    duchyName = duchyName,
    duchyPublicKeys = latestDuchyPublicKeys,
    googleCloudStorage = googleCloudStorage.storage,
    storageBucket = googleCloudStorage.bucket,
    computationStorageServiceChannel = channel
  )

  CommonServer.fromFlags(
    flags.server,
    SERVER_NAME,
    LiquidLegionsComputationControlServiceImpl(storageClients)
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
