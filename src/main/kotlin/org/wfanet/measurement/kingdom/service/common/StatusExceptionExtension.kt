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

package org.wfanet.measurement.kingdom.service.common

import com.google.protobuf.Any as ProtoAny
import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import com.google.rpc.status
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.ErrorCode

fun StatusException.toExternalRuntimeException(
  status: Status,
  description: String,
): StatusRuntimeException {
  val errorInfo = this.errorInfo
  var errorMessage = description
  val metadataMap = mutableMapOf<String, String>()
  if (errorInfo != null)
    if (errorInfo.domain == ErrorCode.getDescriptor().fullName) {
      when (errorInfo.reason) {
        ErrorCode.MEASUREMENT_NOT_FOUND.toString() -> {
          if (
            errorInfo.metadataMap.containsKey("external_measurement_consumer_id") &&
              errorInfo.metadataMap.containsKey("external_measurement_id")
          ) {
            metadataMap["measurement"] =
              MeasurementKey(
                externalIdToApiId(
                  errorInfo.metadataMap["external_measurement_consumer_id"]!!.toLong()
                ),
                externalIdToApiId(errorInfo.metadataMap["external_measurement_id"]!!.toLong()),
              )
              .toName()
            errorMessage = "Measurement ${metadataMap["measurement"]} not found"
          } else {
            errorMessage = "Measurement not found."
          }
        }
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND.toString() -> {
          metadataMap["measurement_consumer"] =
            MeasurementConsumerKey(
              externalIdToApiId(errorInfo.metadataMap["external_measurement_consumer_id"]!!.toLong()),
            )
              .toName()
          errorMessage = "Measurement Consumer ${metadataMap["measurement_consumer"]} not found."
        }
        ErrorCode.DATA_PROVIDER_NOT_FOUND.toString() -> {
          metadataMap["data_provider"] =
            DataProviderKey(
              externalIdToApiId(
                errorInfo.metadataMap["external_data_provider_id"]!!.toLong()
              )
            )
              .toName()
          errorMessage = "Data Provider ${metadataMap["data_provider"]} not found."
        }
        ErrorCode.DUCHY_NOT_FOUND.toString() -> {
          metadataMap["duchy"] =
            DuchyKey(errorInfo.metadataMap["external_duchy_id"].toString()).toName()
          errorMessage = "Duchy ${metadataMap["duchy"]} not found."
        }
        // TODO{@jcorilla}: Look to differentiate certificate types
        ErrorCode.CERTIFICATE_NOT_FOUND.toString() -> {
          metadataMap["certificate"] = errorInfo.metadataMap["external_certificate_id"].toString()
          errorMessage = "Certificate not found."
        }
        ErrorCode.CERTIFICATE_IS_INVALID.toString() -> {
          errorMessage = "Certificate is invalid."
        }
        ErrorCode.DUCHY_NOT_ACTIVE.toString() -> {
          metadataMap["duchy"] =
            DuchyKey(errorInfo.metadataMap["external_duchy_id"]!!.toString()).toName()
          errorMessage = "Duchy ${metadataMap["duchy"]} is not active."
        }
        ErrorCode.MEASUREMENT_STATE_ILLEGAL.toString() -> {
          metadataMap["measurement"] =
            MeasurementKey(
              externalIdToApiId(
                errorInfo.metadataMap["external_measurement_consumer_id"]!!.toLong()
              ),
              externalIdToApiId(errorInfo.metadataMap["external_measurement_id"]!!.toLong()),
              )
              .toName()
          metadataMap["state"] = errorInfo.metadataMap["measurement_state"].toString()
          errorMessage =
            "Measurement ${metadataMap["measurement"]} is in illegal state: ${metadataMap["state"]}"
        }
        else -> {
          metadataMap.putAll(errorInfo.metadataMap)
        }
      }
    } else {
      metadataMap.putAll(errorInfo.metadataMap)
    }
  val statusProto = status {
    code = status.code.value()
    message = errorMessage
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
