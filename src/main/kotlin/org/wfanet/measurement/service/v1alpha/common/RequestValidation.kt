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
