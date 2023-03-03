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

import io.grpc.ManagedChannel
import java.time.Duration
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabase
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseReader
import org.wfanet.measurement.duchy.db.computation.ComputationsDatabaseTransactor
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import org.wfanet.measurement.duchy.service.internal.computations.ComputationsService
import org.wfanet.measurement.duchy.service.internal.computationstats.ComputationStatsService
import org.wfanet.measurement.duchy.storage.ComputationStore
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationStageDetails
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import picocli.CommandLine

private typealias ComputationsDb =
  ComputationsDatabaseTransactor<
    ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
  >

/** gRPC server for Computations service. */
abstract class ComputationsServer : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  abstract val protocolStageEnumHelper:
    ComputationProtocolStagesEnumHelper<ComputationType, ComputationStage>
  abstract val computationProtocolStageDetails:
    ComputationProtocolStageDetailsHelper<
      ComputationType, ComputationStage, ComputationStageDetails, ComputationDetails
    >

  protected fun run(
    computationsDatabaseReader: ComputationsDatabaseReader,
    computationDb: ComputationsDb,
    continuationTokensService: ContinuationTokensCoroutineImplBase,
    storageClient: StorageClient,
  ) {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.server.tlsFlags.certFile,
        privateKeyFile = flags.server.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.server.tlsFlags.certCollectionFile
      )
    val channel: ManagedChannel =
      buildMutualTlsChannel(flags.systemApiFlags.target, clientCerts, flags.systemApiFlags.certHost)
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val computationLogEntriesClient =
      ComputationLogEntriesCoroutineStub(channel).withDuchyId(flags.duchy.duchyName)

    val computationsDatabase = newComputationsDatabase(computationsDatabaseReader, computationDb)
    val computationService =
      ComputationsService(
        computationsDatabase = computationsDatabase,
        computationLogEntriesClient = computationLogEntriesClient,
        computationStorageClient = ComputationStore(storageClient),
        requisitionStorageClient = RequisitionStore(storageClient),
        duchyName = flags.duchy.duchyName
      )

    CommonServer.fromFlags(
        flags.server,
        javaClass.name,
        computationService,
        ComputationStatsService(computationsDatabase),
        continuationTokensService
      )
      .start()
      .blockUntilShutdown()
  }

  private fun newComputationsDatabase(
    computationsDatabaseReader: ComputationsDatabaseReader,
    computationDb: ComputationsDb
  ): ComputationsDatabase {
    return object :
      ComputationsDatabase,
      ComputationsDatabaseReader by computationsDatabaseReader,
      ComputationsDb by computationDb,
      ComputationProtocolStagesEnumHelper<
        ComputationType, ComputationStage
      > by protocolStageEnumHelper {}
  }

  protected class Flags {
    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Mixin
    lateinit var duchy: CommonDuchyFlags
      private set

    @CommandLine.Mixin
    lateinit var duchyInfo: DuchyInfoFlags
      private set

    @CommandLine.Option(
      names = ["--channel-shutdown-timeout"],
      defaultValue = "3s",
      description = ["How long to allow for the gRPC channel to shutdown."],
      required = true
    )
    lateinit var channelShutdownTimeout: Duration
      private set

    @CommandLine.Mixin
    lateinit var systemApiFlags: SystemApiFlags
      private set
  }

  companion object {
    const val SERVICE_NAME = "Computations"
  }
}
