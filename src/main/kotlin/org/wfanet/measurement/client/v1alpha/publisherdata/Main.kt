package org.wfanet.measurement.client.v1alpha.publisherdata

import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
  PublisherDataClient().use {
    it.getCombinedPublicKey()
  }
}
