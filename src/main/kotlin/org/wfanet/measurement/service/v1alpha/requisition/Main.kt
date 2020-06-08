package org.wfanet.measurement.service.v1alpha.requisition

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt

object StubFlags {
  val INTERNAL_API_HOST = stringFlag("internal_api_host", "")
  val INTERNAL_API_PORT = intFlag("internal_api_port", 0)
}

fun main(args: Array<String>) {
  Flags.parse(args.toList())

  val channel: ManagedChannel =
    ManagedChannelBuilder
      .forAddress(StubFlags.INTERNAL_API_HOST.value, StubFlags.INTERNAL_API_PORT.value)
      .usePlaintext()
      .build()

  val stub = RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub(channel)

  CommonServer("RequisitionServer", 8080, RequisitionService(stub))
    .start()
    .blockUntilShutdown()
}
