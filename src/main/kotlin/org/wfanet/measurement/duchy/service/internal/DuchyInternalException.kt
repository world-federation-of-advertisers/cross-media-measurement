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

sealed class DuchyInternalException(val code: ErrorCode, override val message: String) :
  Exception() {
  protected abstract val context: Map<String, String>

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message
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

class ContinuationTokenInvalidException(
  val continuationToken: String,
  message: String,
) : DuchyInternalException(ErrorCode.CONTINUATION_TOKEN_INVALID, message) {
  override val context
    get() = mapOf("continuation_token" to continuationToken)
}

class ContinuationTokenMalformedException(
  val continuationToken: String,
  message: String,
) : DuchyInternalException(ErrorCode.CONTINUATION_TOKEN_MALFORMED, message) {
  override val context
    get() = mapOf("continuation_token" to continuationToken)
}
