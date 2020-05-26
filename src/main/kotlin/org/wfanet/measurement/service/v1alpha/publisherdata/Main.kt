package org.wfanet.measurement.service.v1alpha.publisherdata

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.CommonServerType

fun main() {
  CommonServer(CommonServerType.PUBLISHER_DATA, PublisherDataImpl())
    .start()
    .blockUntilShutdown()
}
