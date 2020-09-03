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

package org.wfanet.measurement.service.v1alpha.globalcomputation

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.service.common.CommonServer
import org.wfanet.measurement.service.v1alpha.common.KingdomApiServerFlags
import org.wfanet.measurement.service.v1alpha.common.runKingdomApiServer
import picocli.CommandLine

@CommandLine.Command(
  name = "global_computations_server",
  description = ["Server daemon for GlobalComputations service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin duchyIdFlags: DuchyIdFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags
) {
  runKingdomApiServer(kingdomApiServerFlags, duchyIdFlags, commonServerFlags) { channel ->
    GlobalComputationService(
      ReportStorageCoroutineStub(channel),
      ReportLogEntryStorageCoroutineStub(channel)
    )
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
