/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.grpc

import com.google.protobuf.Any as ProtoAny
import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import com.google.rpc.status
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ErrorCode

val StatusException.errorInfo: ErrorInfo?
  get() {
    val errorInfoFullName = ErrorInfo.getDescriptor().fullName
    val errorInfoPacked =
      StatusProto.fromStatusAndTrailers(this.status, this.trailers).detailsList.find {
        it.typeUrl.endsWith("/$errorInfoFullName")
      }
    return errorInfoPacked?.unpack(ErrorInfo::class.java)
  }

fun StatusException.toExternalRuntimeException(
  status: Status,
  description: String,
): StatusRuntimeException {
  val errorInfo = this.errorInfo
  val metadataMap = mutableMapOf<String, String>()
  if (errorInfo != null)
    if (errorInfo.domain == ErrorCode.getDescriptor().fullName) {
      errorInfo.metadataMap.forEach { (key, value) ->
        when (key) {
          "external_computation_id" ->
            metadataMap["computation_id"] = externalIdToApiId(value.toLong())
          "external_measurement_id" ->
            metadataMap["measurement_id"] = externalIdToApiId(value.toLong())
          "external_measurement_consumer_id" ->
            metadataMap["measurement_consumer_id"] = externalIdToApiId(value.toLong())
          "external_certificate_id" ->
            metadataMap["certificate_id"] = externalIdToApiId(value.toLong())
          "external_data_provider_id" ->
            metadataMap["data_provider_id"] = externalIdToApiId(value.toLong())
          "external_model_provider_id" ->
            metadataMap["model_provider_id"] = externalIdToApiId(value.toLong())
          else -> metadataMap[key] = value
        }
      }
    } else {
      metadataMap.putAll(errorInfo.metadataMap)
    }
  val statusProto =
    status {
      code = status.code.value()
      message = description
      details +=
        ProtoAny.pack(
          errorInfo {
            reason = this@toExternalRuntimeException.status.code.toString()
            domain = ErrorInfo.getDescriptor().fullName
            metadata.putAll(metadataMap)
          }
        )
    }
  return StatusProto.toStatusRuntimeException(statusProto)
}
