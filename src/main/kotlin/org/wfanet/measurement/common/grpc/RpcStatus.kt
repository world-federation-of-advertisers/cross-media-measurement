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

package org.wfanet.measurement.common.grpc

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.throttler.ThrottledException
import org.wfanet.measurement.common.throttler.Throttler

/**
 * Variant of [Throttler.onReady] that converts gRPC [StatusRuntimeException]s to
 * [ThrottledException]s for the appropriate codes.
 */
suspend fun <T> Throttler.onReadyGrpc(block: suspend () -> T): T =
  onReady {
    try {
      block()
    } catch (e: StatusRuntimeException) {
      when (e.status.code) {
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
        Status.Code.UNAVAILABLE -> throw ThrottledException("gRPC back-off: ${e.status}", e)
        else -> throw e
      }
    }
  }

/**
 * Attempts to extract the gRPC status code from an [Exception]. If it cannot, it rethrows.
 */
fun Throwable.grpcStatusCode(): Status.Code? {
  return when (this) {
    is StatusRuntimeException -> status.code
    is StatusException -> status.code
    else -> return null
  }
}
