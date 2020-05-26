package org.wfanet.measurement.service.v1alpha.computationcontrol

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.CommonServerType

fun main() {
  CommonServer(CommonServerType.COMPUTATION_CONTROL, ComputationControlImpl())
    .start()
    .blockUntilShutdown()
}
