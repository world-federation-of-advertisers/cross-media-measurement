// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common.server

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineStub as InternalMeasurementLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationLogEntriesService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationParticipantsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.RequisitionsService
import picocli.CommandLine

private const val SERVER_NAME = "SystemApiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Kingdom system API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin duchyInfoFlags: DuchyInfoFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin serviceFlags: ServiceFlags,
) {
  val serviceDispatcher: CoroutineDispatcher = serviceFlags.executor.asCoroutineDispatcher()
  runKingdomApiServer(kingdomApiServerFlags, SERVER_NAME, duchyInfoFlags, commonServerFlags) {
    channel ->
    listOf(
      ComputationsService(InternalMeasurementsCoroutineStub(channel), serviceDispatcher),
      ComputationParticipantsService(
        InternalComputationParticipantsCoroutineStub(channel),
        serviceDispatcher,
      ),
      ComputationLogEntriesService(
        InternalMeasurementLogEntriesCoroutineStub(channel),
        serviceDispatcher,
      ),
      RequisitionsService(InternalRequisitionsCoroutineStub(channel), serviceDispatcher),
    )
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
