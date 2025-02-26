// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.securecomputation.ErrorCode

sealed class SecurecomputationInternalException : Exception {

  val code: ErrorCode
  protected abstract val context: Map<String, String>

  constructor(code: ErrorCode, message: String, cause: Throwable? = null) : super(message, cause) {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : this(code, message = buildMessage())

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message!!,
  ): StatusRuntimeException {
    val errorInfo = errorInfo {
      reason = this@SecurecomputationInternalException.code.toString()
      domain = ErrorCode.getDescriptor().fullName
      metadata.putAll(context)
    }
    return Errors.buildStatusRuntimeException(statusCode, message, errorInfo, this)
  }

  override fun toString(): String {
    return super.toString() + " " + context.toString()
  }
}

class WorkItemNotFoundException(
  val externalWorkItemId: ExternalId,
  provideDescription: () -> String = { "WorkItem not found" },
) : SecurecomputationInternalException(ErrorCode.WORK_ITEM_NOT_FOUND, provideDescription) {
  override val context
    get() =
      mapOf("external_measurement_consumer_id" to externalWorkItemId.value.toString())
}
