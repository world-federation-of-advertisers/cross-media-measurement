package org.wfanet.measurement.client.v1alpha.publisherdata

fun main() {
  PublisherDataClient().use {
    it.getCombinedPublicKey()
    it.createMetric()
  }
}