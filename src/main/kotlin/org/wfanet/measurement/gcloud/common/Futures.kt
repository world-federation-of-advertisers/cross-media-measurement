// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.gcloud.common

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
import java.util.concurrent.Executor
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred

/** Wraps the [ApiFuture] in a [Deferred] using the specified [executor]. */
fun <T> ApiFuture<T>.asDeferred(executor: Executor): Deferred<T> {
  val deferred = CompletableDeferred<T>()
  ApiFutures.addCallback(
    this,
    object : ApiFutureCallback<T> {
      override fun onFailure(e: Throwable) {
        deferred.completeExceptionally(e)
      }

      override fun onSuccess(result: T) {
        deferred.complete(result)
      }
    },
    executor
  )
  return deferred
}
