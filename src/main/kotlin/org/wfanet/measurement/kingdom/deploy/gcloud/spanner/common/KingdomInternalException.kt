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

const val CONTEXT_KEY_ID = "id"
const val CONTEXT_KEY_NAME = "NAME"
const val CONTEXT_KEY_STATE = "STATE"


/* Throw internal exceptions with reserved parameters

Throw internal exception:
throwMeasurementConsumerNotFound(id="123") { "measurement_consumer not existing" }

Catch internal exception and throw Grpc runtime exception to the client:
catch(e: KingdomInternalException) {
  when(e) {
    ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND ->
          e.throwRuntimeException(Status.FAILED_PRECONDITION) { "MeasurementConsumer not found" }
    else -> {}
  }
}

The client receive the Grpc runtime exception and check reason and context:
catch(e: StatusRuntimeException) {
   val info = e.getErrorInfo()
   if(info.notNull() && info.reason = MEASUREMENT_CONSUMER_NOT_FOUND.getName()) {
       val measurementConsumerId = info.metadata["id"]
       blame(measurementConsumerId)
   }
}
 */

class KingdomInternalException : Exception {
  val code: ErrorCode
  lateinit var context: Map<String, String>

  constructor(code: ErrorCode, context: Map<String, String> = emptyMap()) : super() {
    this.code = code
    this.context = context
  }

  constructor(code: ErrorCode, context: Map<String, String> = emptyMap(), buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
    this.context = context
  }

  fun throwRuntimeException(
    status: Status = Status.INVALID_ARGUMENT,
    provideDescription: () -> String) {

    throwRuntimeException(status, code, context, provideDescription)
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

fun StatusRuntimeException.getErrorInfo(): ErrorInfo? {
  val key = ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance())
  return trailers?.get(key)
}

fun StatusRuntimeException.getErrorContext(): Map<String, String> {
  val key = ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance())
  return trailers?.get(key)?.metadataMap ?: emptyMap()
}


private fun addIdToErrorContext(details: MutableMap<String, String>, id: String) {
  details[CONTEXT_KEY_ID] = id
}

private fun addNameToErrorContext(details: MutableMap<String, String>, name: String) {
  details[CONTEXT_KEY_NAME] = name
}

private fun addStateToErrorContext(details: MutableMap<String, String>, state: Int) {
  details[CONTEXT_KEY_STATE] = state.toString()
}

fun throwMeasurementConsumerNotFound(id: String, provideDescription: () -> String) {
  val context = mutableMapOf<String, String>()
  addIdToErrorContext(context, id)
  throw KingdomInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND, context) { provideDescription() }
}


