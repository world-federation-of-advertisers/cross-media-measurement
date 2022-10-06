package org.wfanet.measurement.duchy.daemon.herald.testing

import org.wfanet.measurement.duchy.daemon.herald.ContinuationTokenStore

class InMemoryContinuationTokenStore() : ContinuationTokenStore {
  var latestContinuationToken = ""

  override suspend fun readContinuationToken(): String {
    return latestContinuationToken
  }

  override suspend fun updateContinuationToken(continuationToken: String) {
    latestContinuationToken = continuationToken
  }
}
