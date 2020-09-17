// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.identity.testing

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.wfanet.measurement.common.identity.DuchyIdentity

private typealias DuchyIdProvider = () -> DuchyIdentity

/**
 * Maintains a sender [DuchyIdentity] for calls to a given service
 * implementation.
 */
class SenderContext<T>(serviceProvider: (DuchyIdProvider) -> T) {
  private val mutex = Mutex()

  lateinit var sender: DuchyIdentity
    private set

  val service: T = serviceProvider { sender }

  suspend fun <R> withSender(
    sender: DuchyIdentity,
    callMethod: suspend T.() -> R
  ): R = mutex.withLock {
    this.sender = sender
    service.callMethod()
  }
}
