// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.internal

import com.google.protobuf.Any
import com.google.rpc.errorInfo
import com.google.rpc.status
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.wfanet.measurement.internal.duchy.ErrorCode

sealed class DuchyInternalException(
  val code: ErrorCode,
  message: String,
  cause: Throwable? = null,
) : Exception(message, cause) {
  override val message: String
    get() = super.message!!

  protected abstract val context: Map<String, String>

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message,
  ): StatusRuntimeException {
    val statusProto = status {
      code = statusCode.value()
      this.message = message
      details +=
        Any.pack(
          errorInfo {
            reason = this@DuchyInternalException.code.toString()
            domain = ErrorCode.getDescriptor().fullName
            metadata.putAll(context)
          }
        )
    }
    return StatusProto.toStatusRuntimeException(statusProto)
  }
}

class ContinuationTokenInvalidException(val continuationToken: String, message: String) :
  DuchyInternalException(ErrorCode.CONTINUATION_TOKEN_INVALID, message) {
  override val context
    get() = mapOf("continuation_token" to continuationToken)
}

class ContinuationTokenMalformedException(val continuationToken: String, message: String) :
  DuchyInternalException(ErrorCode.CONTINUATION_TOKEN_MALFORMED, message) {
  override val context
    get() = mapOf("continuation_token" to continuationToken)
}

class ComputationNotFoundException(
  val computationId: Long,
  message: String = "Computation not found",
) : DuchyInternalException(ErrorCode.COMPUTATION_NOT_FOUND, message) {
  override val context
    get() = mapOf("computation_id" to computationId.toString())
}

class ComputationDetailsNotFoundException(
  val computationId: Long,
  val computationStage: String,
  val attempt: Long,
  message: String = "ComputationDetails not found",
) : DuchyInternalException(ErrorCode.COMPUTATION_DETAILS_NOT_FOUND, message) {
  override val context
    get() =
      mapOf(
        "computation_id" to computationId.toString(),
        "computation_stage" to computationStage,
        "attempt" to attempt.toString(),
      )
}

class ComputationInitialStageInvalidException(
  val protocol: String,
  val computationStage: String,
  message: String = "Computation initial stage is invalid",
) : DuchyInternalException(ErrorCode.COMPUTATION_DETAILS_NOT_FOUND, message) {
  override val context
    get() = mapOf("protocol" to protocol, "computation_stage" to computationStage)
}

class ComputationAlreadyExistsException(
  val globalComputationId: String,
  message: String = "Computation already exists",
) : DuchyInternalException(ErrorCode.COMPUTATION_ALREADY_EXISTS, message) {
  override val context
    get() = mapOf("global_computation_id" to globalComputationId)
}

class ComputationTokenVersionMismatchException(
  val computationId: Long,
  val version: Long,
  val tokenVersion: Long,
  message: String = "ComputationToken version mismatch",
) : DuchyInternalException(ErrorCode.COMPUTATION_TOKEN_VERSION_MISMATCH, message) {
  override val context
    get() =
      mapOf(
        "computation_id" to computationId.toString(),
        "version" to version.toString(),
        "token_version" to tokenVersion.toString(),
      )
}

class ComputationLockOwnerMismatchException(
  val computationId: Long,
  val expectedOwner: String,
  val actualOwner: String?,
  message: String =
    "Lock owner mismatch for computation $computationId. " +
      "Expected owner '$expectedOwner', but found '$actualOwner'.",
) : DuchyInternalException(ErrorCode.COMPUTATION_LOCK_OWNER_MISMATCH, message) {
  override val context
    get() =
      mapOf(
        "computation_id" to computationId.toString(),
        "expected_owner" to expectedOwner,
        // Provide a clear string representation if the lock is currently unowned.
        "actual_owner" to (actualOwner ?: "none"),
      )
}
