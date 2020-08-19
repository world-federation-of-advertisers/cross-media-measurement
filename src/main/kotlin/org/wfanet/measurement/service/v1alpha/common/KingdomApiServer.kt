package org.wfanet.measurement.service.v1alpha.common

import io.grpc.BindableService
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.DuchyIdFlags
import org.wfanet.measurement.common.DuchyIds
import picocli.CommandLine

class KingdomApiServerFlags {
  @set:CommandLine.Option(
    names = ["--internal-api-target"],
    description = ["Target for Kingdom database APIs, e.g. localhost:8080"],
    required = true
  )
  lateinit var internalApiTarget: String
}

fun runKingdomApiServer(
  kingdomApiServerFlags: KingdomApiServerFlags,
  duchyIdFlags: DuchyIdFlags,
  commonServerFlags: CommonServer.Flags,
  serviceFactory: (ManagedChannel) -> BindableService
) {
  DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)

  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forTarget(kingdomApiServerFlags.internalApiTarget)
      .usePlaintext()
      .build()

  val service = serviceFactory(channel)
  val name = service.bindService().serviceDescriptor.name + "Server"

  CommonServer
    .fromFlags(commonServerFlags, name, service)
    .start()
    .blockUntilShutdown()
}
