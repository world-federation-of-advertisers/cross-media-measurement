// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc

import io.grpc.ServerCall

/** gRPC [ServerCall.Listener] that defers event actions */
class DeferredListener<ReqT> : ServerCall.Listener<ReqT>() {
  private lateinit var delegate: ServerCall.Listener<ReqT>
  private val events = arrayListOf<Runnable>()

  @Synchronized
  override fun onMessage(message: ReqT) {
    if (!this::delegate.isInitialized) {
      events.add { delegate.onMessage(message) }
    } else {
      delegate.onMessage(message)
    }
  }

  @Synchronized
  override fun onHalfClose() {
    if (!this::delegate.isInitialized) {
      events.add { delegate.onHalfClose() }
    } else {
      delegate.onHalfClose()
    }
  }

  @Synchronized
  override fun onCancel() {
    if (!this::delegate.isInitialized) {
      events.add { delegate.onCancel() }
    } else {
      delegate.onCancel()
    }
  }

  @Synchronized
  override fun onComplete() {
    if (!this::delegate.isInitialized) {
      events.add { delegate.onComplete() }
    } else {
      delegate.onComplete()
    }
  }

  @Synchronized
  override fun onReady() {
    if (!this::delegate.isInitialized) {
      events.add { delegate.onReady() }
    } else {
      delegate.onReady()
    }
  }

  @Synchronized
  fun setDelegate(delegate: ServerCall.Listener<ReqT>) {
    this.delegate = delegate
    events.forEach { it.run() }
    events.clear()
  }
}
