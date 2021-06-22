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

package org.wfanet.measurement.kingdom.deploy.common.server

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.internal.kingdom.ReportLogEntriesGrpcKt.ReportLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.kingdom.service.system.v1alpha.GlobalComputationService
import picocli.CommandLine

@CommandLine.Command(
  name = "global_computations_server",
  description = ["Server daemon for GlobalComputations service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin duchyInfoFlags: DuchyInfoFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags
) {
  runKingdomApiServer(kingdomApiServerFlags, duchyInfoFlags, commonServerFlags) { channel ->
    GlobalComputationService(ReportsCoroutineStub(channel), ReportLogEntriesCoroutineStub(channel))
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
