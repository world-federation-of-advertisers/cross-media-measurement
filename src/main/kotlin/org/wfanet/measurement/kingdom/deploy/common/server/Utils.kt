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

import io.grpc.BindableService
import io.grpc.Channel
import kotlin.properties.Delegates
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.common.identity.withDuchyIdentities
import org.wfanet.measurement.kingdom.deploy.common.InternalApiFlags
import picocli.CommandLine

class KingdomApiServerFlags {
  @CommandLine.Mixin
  lateinit var internalApiFlags: InternalApiFlags
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false"
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set
}

fun runKingdomApiServer(
  kingdomApiServerFlags: KingdomApiServerFlags,
  serverName: String,
  duchyInfoFlags: DuchyInfoFlags,
  commonServerFlags: CommonServer.Flags,
  serviceFactory: (Channel) -> Iterable<BindableService>
) {
  DuchyInfo.initializeFromFlags(duchyInfoFlags)

  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile
    )
  val channel: Channel =
    buildMutualTlsChannel(
        kingdomApiServerFlags.internalApiFlags.target,
        clientCerts,
        kingdomApiServerFlags.internalApiFlags.certHost
      )
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)
      .withDefaultDeadline(kingdomApiServerFlags.internalApiFlags.defaultDeadlineDuration)
  val service = serviceFactory(channel).map { it.withDuchyIdentities() }

  CommonServer.fromFlags(commonServerFlags, serverName, service).start().blockUntilShutdown()
}
