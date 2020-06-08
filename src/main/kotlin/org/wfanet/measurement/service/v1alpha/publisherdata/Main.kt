package org.wfanet.measurement.service.v1alpha.publisherdata

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.stringFlag

fun main(args: Array<String>) {
  val port = intFlag("port", 8080)
  val nameForLogging = stringFlag("name-for-logging", "PublisherDataServer")
  Flags.parse(args.asIterable())

  CommonServer(nameForLogging.value, port.value, PublisherDataImpl())
    .start()
    .blockUntilShutdown()
}
