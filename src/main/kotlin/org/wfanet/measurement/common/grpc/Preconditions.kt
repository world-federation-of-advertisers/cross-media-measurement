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
import io.grpc.StatusRuntimeException

/**
 * Throws [StatusRuntimeException] with [Status.INVALID_ARGUMENT] if [predicate]
 * returns `false`.
 *
 * This is the gRPC equivalent of [require].
 *
 * @param predicate throw if this is false
 * @param provideDescription lazy generator for the error message
 * @throws StatusRuntimeException if [predicate] is false
 */
fun grpcRequire(
  predicate: Boolean,
  provideDescription: () -> String
) {
  if (!predicate) failGrpc(Status.INVALID_ARGUMENT, provideDescription)
}

/**
 * Throws a [StatusRuntimeException] with [Status.INVALID_ARGUMENT] if [subject]
 * is `null`.
 *
 * This is the gRPC equivalent of [requireNotNull].
 *
 * @return the non-null [subject]
 */
fun <T> grpcRequireNotNull(
  subject: T?,
  provideDescription: () -> String = { "" }
): T {
  return subject ?: failGrpc(Status.INVALID_ARGUMENT, provideDescription)
}

/**
 * Throws [StatusRuntimeException] with a description.
 *
 * @param status what gRPC error code to use
 * @param provideDescription lazy generator for the error message
 * @throws StatusRuntimeException
 */
fun failGrpc(status: Status = Status.INVALID_ARGUMENT, provideDescription: () -> String): Nothing =
  throw status.withDescription(provideDescription()).asRuntimeException()
