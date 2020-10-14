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

package org.wfanet.measurement.service.internal.duchy.computation.storage

import java.time.Duration
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import org.wfanet.measurement.common.buildChannel
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.db.duchy.computation.ComputationsRelationalDb
import org.wfanet.measurement.db.duchy.computation.ProtocolStageDetails
import org.wfanet.measurement.db.duchy.computation.ProtocolStageEnumHelper
import org.wfanet.measurement.db.duchy.computation.ReadOnlyComputationsRelationalDb
import org.wfanet.measurement.db.duchy.computation.SingleProtocolDatabase
import org.wfanet.measurement.duchy.CommonDuchyFlags
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.service.common.CommonServer
import picocli.CommandLine

/** gRPC server for ComputationStorage service. */
abstract class ComputationStorageServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  protected val duchyPublicKeys by lazy {
    DuchyPublicKeys.fromFlags(flags.duchyPublicKeys)
  }

  abstract val computationType: ComputationType
  abstract val stageEnumHelper: ProtocolStageEnumHelper<ComputationStage>
  abstract val stageDetails: ProtocolStageDetails<ComputationStage, ComputationStageDetails>

  protected fun run(
    readOnlyComputationDb: ReadOnlyComputationsRelationalDb,
    computationDb: ComputationsRelationalDb<ComputationStage, ComputationStageDetails>
  ) {
    val channel = buildChannel(flags.globalComputationsService, flags.channelShutdownTimeout)

    val globalComputationsServiceClient = GlobalComputationsCoroutineStub(channel)
      .withDuchyId(flags.duchy.duchyName)

    CommonServer.fromFlags(
      flags.server,
      javaClass.name,
      ComputationStorageServiceImpl(
        computationsDatabase = newSingleProtocolDb(readOnlyComputationDb, computationDb),
        globalComputationsClient = globalComputationsServiceClient,
        duchyName = flags.duchy.duchyName
      )
    ).start().blockUntilShutdown()
  }

  private fun newSingleProtocolDb(
    readOnlyComputationDb: ReadOnlyComputationsRelationalDb,
    computationDb: ComputationsRelationalDb<ComputationStage, ComputationStageDetails>
  ): SingleProtocolDatabase {
    return object :
      SingleProtocolDatabase,
      ReadOnlyComputationsRelationalDb by readOnlyComputationDb,
      ComputationsRelationalDb<ComputationStage, ComputationStageDetails> by computationDb,
      ProtocolStageEnumHelper<ComputationStage> by stageEnumHelper {
      override val computationType = this@ComputationStorageServer.computationType
    }
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
      names = ["--channel-shutdown-timeout"],
      defaultValue = "3s",
      description = ["How long to allow for the gRPC channel to shutdown."],
      required = true
    )
    lateinit var channelShutdownTimeout: Duration
      private set

    @CommandLine.Option(
      names = ["--global-computation-service-target"],
      description = ["Address and port of the Global Computation Service"],
      required = true
    )
    lateinit var globalComputationsService: String
      private set
  }

  companion object {
    const val SERVICE_NAME = "ComputationStorage"
  }
}
