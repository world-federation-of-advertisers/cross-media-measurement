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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.ProtoUtils
import org.wfanet.measurement.internal.kingdom.ErrorCode

const val KEY_MEASUREMENT_CONSUMER_ID = "measurement_consumer_id"

class KingdomInternalException : Exception {
  val code: ErrorCode

  constructor(code: ErrorCode) : super() {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }
}

fun throwRuntimeException(
  status: Status = Status.INVALID_ARGUMENT,
  code: ErrorCode,
  context: Map<String, String> = emptyMap(),
  provideDescription: () -> String) {

  val info = errorInfo {
    reason = code.toString()
    domain = ErrorInfo::class.qualifiedName.toString()
    metadata.putAll(context)
  }

  val metadata = Metadata()
  metadata.put(ProtoUtils.keyForProto(info), info)

  throw status.withDescription(provideDescription()).asRuntimeException(metadata)
}

fun getErrorInfo(error: StatusRuntimeException): ErrorInfo? {
  val key = ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance())
  return error.trailers?.get(key)
}

private fun addIdToErrorContext(details: Map<String, String>, id: String) {

}

private fun addNameToErrorContext(details: Map<String, String>, name: String) {

}

private fun addStateToErrorContext(details: Map<String, String>, state: Int) {

}

fun throwMeasurementConsumerNotFound(id: String, provideDescription: () -> String) {
  val details: Map<String, String> = emptyMap()
  addIdToErrorContext(details, id)
  throwRuntimeException(
    Status.FAILED_PRECONDITION,
    ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
    details,
    provideDescription
  )
}


