package org.wfanet.measurement.common

import io.grpc.Status
import io.grpc.StatusRuntimeException

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
