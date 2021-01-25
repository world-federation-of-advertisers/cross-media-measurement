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

package org.wfanet.measurement.duchy.deploy.common.server

import io.grpc.ManagedChannel
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetails
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.service.internal.computationcontrol.AsyncComputationControlService
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import picocli.CommandLine

private const val SERVICE_NAME = "AsyncComputationControl"
private const val SERVER_NAME = "${SERVICE_NAME}Server"

class AsyncComputationControlServiceFlags {
  @CommandLine.Mixin
  lateinit var server: CommonServer.Flags
    private set

  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyPublicKeys: DuchyPublicKeys.Flags
    private set

  @CommandLine.Mixin
  lateinit var duchyIdFlags: DuchyIdFlags
    private set

  @CommandLine.Option(
    names = ["--computations-service-target"],
    description = ["Address and port of the Computations service"],
    required = true
  )
  lateinit var computationsServiceTarget: String
    private set
}

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for $SERVICE_NAME service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin flags: AsyncComputationControlServiceFlags
) {
  val duchyName = flags.duchy.duchyName
  val latestDuchyPublicKeys = DuchyPublicKeys.fromFlags(flags.duchyPublicKeys).latest
  require(latestDuchyPublicKeys.containsKey(duchyName)) {
    "Public key not specified for Duchy $duchyName"
  }
  DuchyIds.setDuchyIdsFromFlags(flags.duchyIdFlags)
  require(latestDuchyPublicKeys.keys.toSet() == DuchyIds.ALL)

  val otherDuchyNames = latestDuchyPublicKeys.keys.filter { it != duchyName }
  val channel: ManagedChannel = buildChannel(flags.computationsServiceTarget)

  CommonServer.fromFlags(
    flags.server,
    SERVER_NAME,
    AsyncComputationControlService(
      ComputationsCoroutineStub(channel),
      ComputationProtocolStageDetails(otherDuchyNames)
    )
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
