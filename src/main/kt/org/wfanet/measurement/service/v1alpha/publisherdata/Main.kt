package org.wfanet.measurement.service.v1alpha.publisherdata

fun main() {
  val server = PublisherDataServer()
  server.start()
  server.blockUntilShutdown()
}