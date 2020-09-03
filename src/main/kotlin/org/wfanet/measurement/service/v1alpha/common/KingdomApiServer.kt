package org.wfanet.measurement.service.v1alpha.common

import io.grpc.BindableService
import io.grpc.Channel
import io.grpc.ManagedChannelBuilder
import kotlin.properties.Delegates
import org.wfanet.measurement.common.identity.DuchyIdFlags
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.service.common.CommonServer
import org.wfanet.measurement.service.common.withVerboseLogging
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

  val channel =
    ManagedChannelBuilder
      .forTarget(kingdomApiServerFlags.internalApiTarget)
      .usePlaintext()
      .build()
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)

  val service = serviceFactory(channel)
  val name = service.bindService().serviceDescriptor.name + "Server"

  CommonServer
    .fromFlags(commonServerFlags, name, service)
    .start()
    .blockUntilShutdown()
}
