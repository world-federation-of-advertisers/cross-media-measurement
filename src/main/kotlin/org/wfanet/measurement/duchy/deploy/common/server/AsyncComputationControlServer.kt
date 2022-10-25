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

import io.grpc.Channel
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
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
  lateinit var duchyInfo: DuchyInfoFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set
}

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for $SERVICE_NAME service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: AsyncComputationControlServiceFlags) {
  DuchyInfo.initializeFromFlags(flags.duchyInfo)

  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.server.tlsFlags.certFile,
      privateKeyFile = flags.server.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.server.tlsFlags.certCollectionFile
    )

  val channel: Channel =
    buildMutualTlsChannel(
        flags.computationsServiceFlags.target,
        clientCerts,
        flags.computationsServiceFlags.certHost
      )
      .withDefaultDeadline(flags.computationsServiceFlags.defaultDeadlineDuration)

  CommonServer.fromFlags(
      flags.server,
      SERVER_NAME,
      AsyncComputationControlService(ComputationsCoroutineStub(channel))
    )
    .start()
    .blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
