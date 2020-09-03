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
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.crypto.DuchyPublicKeys
import org.wfanet.measurement.db.duchy.computation.ComputationsBlobDb
import org.wfanet.measurement.db.duchy.computation.LiquidLegionsSketchAggregationComputationStorageClients
import org.wfanet.measurement.duchy.CommonDuchyFlags
import org.wfanet.measurement.internal.LiquidLegionsSketchAggregationStage
import org.wfanet.measurement.internal.duchy.ComputationStorageServiceGrpcKt.ComputationStorageServiceCoroutineStub
import org.wfanet.measurement.service.common.CommonServer
import picocli.CommandLine

abstract class LiquidLegionsComputationControlServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  protected fun run(
    computationStore: ComputationsBlobDb<LiquidLegionsSketchAggregationStage>
  ) {
    val duchyName = flags.duchy.duchyName
    val latestDuchyPublicKeys = DuchyPublicKeys.fromFlags(flags.duchyPublicKeys).latest
    require(latestDuchyPublicKeys.containsKey(duchyName)) {
      "Public key not specified for Duchy $duchyName"
    }

    val otherDuchyNames = latestDuchyPublicKeys.keys.filter { it != duchyName }
    val channel: ManagedChannel =
      ManagedChannelBuilder
        .forTarget(flags.computationStorageServiceTarget)
        .usePlaintext()
        .build()

    CommonServer.fromFlags(
      flags.server,
      javaClass.name,
      LiquidLegionsComputationControlServiceImpl(
        LiquidLegionsSketchAggregationComputationStorageClients(
          ComputationStorageServiceCoroutineStub(channel).withDuchyId(duchyName),
          computationStore,
          otherDuchyNames
        )
      )
    ).start().blockUntilShutdown()
  }

  protected class Flags {
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
      description = ["Address and port of the Computation Storage Service"],
      required = true
    )
    lateinit var computationStorageServiceTarget: String
      private set
  }

  companion object {
    const val SERVICE_NAME = "ComputationControl"
  }
}
