// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.datawatcher

/** HTTP endpoint failure returned by a DataWatcher sink. */
class DataWatcherHttpException(val statusCode: Int, message: String) :
  IllegalStateException(message)

/** Retry classification shared by DataWatcher and synchronous replay callers. */
object DataWatcherRetryPolicy {
  private val RETRYABLE_HTTP_STATUS_CODES = setOf(408, 429, 500, 502, 503, 504)

  fun isRetryableHttpFailure(throwable: Throwable): Boolean =
    throwable is DataWatcherHttpException && throwable.statusCode in RETRYABLE_HTTP_STATUS_CODES
}
