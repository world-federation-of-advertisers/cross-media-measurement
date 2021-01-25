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

package org.wfanet.measurement.duchy.deploy.common.server

import java.time.Duration
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsRelationalDb
import org.wfanet.measurement.duchy.db.computation.ReadOnlyComputationsRelationalDb
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.service.internal.computation.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.duchy.toDuchyOrder
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.system.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineStub
import picocli.CommandLine

private typealias ComputationsDb =
  ComputationsRelationalDb<
    ComputationType,
    ComputationStage,
    ComputationStageDetails,
    ComputationDetails
    >

/** gRPC server for Computations service. */
abstract class ComputationsServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  protected val duchyPublicKeys by lazy {
    DuchyPublicKeys.fromFlags(flags.duchyPublicKeys)
  }

  abstract val protocolStageEnumHelper:
    ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
  abstract val computationProtocolStageDetails:
    ComputationProtocolStageDetailsHelper<
      ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails>

  protected fun run(
    readOnlyComputationDb: ReadOnlyComputationsRelationalDb,
    computationDb: ComputationsDb
  ) {
    val channel = buildChannel(flags.globalComputationsServiceTarget, flags.channelShutdownTimeout)

    val globalComputationsClient = GlobalComputationsCoroutineStub(channel)
      .withDuchyId(flags.duchy.duchyName)

    val computationsDatabase = newComputationsDatabase(readOnlyComputationDb, computationDb)
    CommonServer.fromFlags(
      flags.server,
      javaClass.name,
      ComputationsService(
        computationsDatabase = computationsDatabase,
        globalComputationsClient = globalComputationsClient,
        duchyName = flags.duchy.duchyName,
        duchyOrder = duchyPublicKeys.latest.toDuchyOrder()
      ),
      ComputationStatsService(computationsDatabase)
    ).start().blockUntilShutdown()
  }

  private fun newComputationsDatabase(
    readOnlyComputationDb: ReadOnlyComputationsRelationalDb,
    computationDb: ComputationsDb
  ): ComputationsDatabase {
    return object :
      ComputationsDatabase,
      ReadOnlyComputationsRelationalDb by readOnlyComputationDb,
      ComputationsDb by computationDb,
      ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
      by protocolStageEnumHelper {
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
    lateinit var globalComputationsServiceTarget: String
      private set
  }

  companion object {
    const val SERVICE_NAME = "Computations"
  }
}
