package org.wfanet.measurement.service.v1alpha.common

import io.grpc.Status
import io.grpc.StatusRuntimeException

/**
 * Throws [StatusRuntimeException] if the [condition] is false.
 *
 * @param[condition] throw if this is false
 * @param[status] what gRPC error code to use
 * @param[block] lazy generator for the error message
 * @throws[StatusRuntimeException] if [condition] is false
 */
fun grpcRequire(
  condition: Boolean,
  status: Status = Status.INVALID_ARGUMENT,
  block: () -> String
) {
  if (!condition) throw StatusRuntimeException(status.withDescription(block()))
}
