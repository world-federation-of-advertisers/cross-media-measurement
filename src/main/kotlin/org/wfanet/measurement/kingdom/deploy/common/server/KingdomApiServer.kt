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
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.common.identity.withDuchyIdentities
import picocli.CommandLine

class KingdomApiServerFlags {
  @set:CommandLine.Option(
    names = ["--internal-api-target"],
    description = ["Target for Kingdom database APIs, e.g. localhost:8080"],
    required = true
  )
  lateinit var internalApiTarget: String

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
  duchyIdFlags: DuchyIdFlags,
  commonServerFlags: CommonServer.Flags,
  serviceFactory: (Channel) -> BindableService
) {
  DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)

  val channel: Channel =
    buildChannel(kingdomApiServerFlags.internalApiTarget)
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)

  val service = serviceFactory(channel).withDuchyIdentities()
  val name = service.serviceDescriptor.name + "Server"

  CommonServer
    .fromFlags(commonServerFlags, name, service)
    .start()
    .blockUntilShutdown()
}
