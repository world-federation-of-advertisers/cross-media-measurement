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

package org.wfanet.measurement.reporting.service.internal

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.ProtoUtils
import org.wfanet.measurement.internal.reporting.ErrorCode

sealed class ReportingInternalException : Exception {
  val code: ErrorCode
  protected abstract val context: Map<String, String>

  constructor(code: ErrorCode) : super() {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }

  fun throwStatusRuntimeException(
    status: Status = Status.INVALID_ARGUMENT,
    provideDescription: () -> String,
  ): Nothing {
    val info = errorInfo {
      reason = code.toString()
      domain = ErrorInfo::class.qualifiedName.toString()
      metadata.putAll(context)
    }

    val metadata = Metadata()
    metadata.put(ProtoUtils.keyForProto(info), info)

    throw status
      .withDescription(provideDescription() + contextToString())
      .asRuntimeException(metadata)
  }

  private fun contextToString() = context.entries.joinToString(prefix = " ", separator = " ")
}

fun StatusRuntimeException.getErrorInfo(): ErrorInfo? {
  val key = ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance())
  return trailers?.get(key)
}

class ReportingSetAlreadyExistsException(
  provideDescription: () -> String = { "Reporting Set already exists" }
) : ReportingInternalException(ErrorCode.REPORTING_SET_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}
