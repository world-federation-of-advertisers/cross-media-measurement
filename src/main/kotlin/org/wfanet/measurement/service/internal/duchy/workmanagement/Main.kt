package org.wfanet.measurement.service.internal.duchy.workmanagement

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.stringFlag

fun main(args: Array<String>) {
  val port = intFlag("port", 8080)
  val nameForLogging = stringFlag("name-for-logging", "WorkManagementServer")
  Flags.parse(args.asIterable())

  CommonServer(nameForLogging.value, port.value, WorkManagementServiceImpl())
    .start()
    .blockUntilShutdown()
}
