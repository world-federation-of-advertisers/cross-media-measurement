package org.wfanet.measurement.server.v1alpha.publisherdata

fun main() {
  val server =
    PublisherDataServer()
  server.start()
  server.blockUntilShutdown()
}
