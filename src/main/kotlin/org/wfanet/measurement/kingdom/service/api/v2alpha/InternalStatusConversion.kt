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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.Any as ProtoAny
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

/**
 * Converts this [Status] to a [StatusRuntimeException] with details from [internalApiException].
 * This may replace the error info and description...
 */
fun Status.toExternalStatusRuntimeException(
  internalApiException: StatusException
): StatusRuntimeException {
  val errorInfo = internalApiException.errorInfo

  if (errorInfo == null || errorInfo.domain != ErrorCode.getDescriptor().fullName) {
    return this.asRuntimeException()
  }
  var errorMessage = this.description ?: "Unknown exception."
  val metadataMap =
    buildMap<String, String> {
      when (ErrorCode.valueOf(errorInfo.reason)) {
        ErrorCode.MEASUREMENT_NOT_FOUND -> {
          put(
            "measurement",
            MeasurementKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                ),
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_id"]).toLong()
                ),
              )
              .toName(),
          )
          errorMessage = "Measurement ${get("measurement")} not found"
        }
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND -> {
          put(
            "measurement_consumer",
            MeasurementConsumerKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                )
              )
              .toName(),
          )
          errorMessage = "Measurement Consumer ${get("measurement_consumer")} not found."
        }
        ErrorCode.DATA_PROVIDER_NOT_FOUND -> {
          put(
            "data_provider",
            DataProviderKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_data_provider_id"]).toLong()
                )
              )
              .toName(),
          )
          errorMessage = "Data Provider ${get("data_provider")} not found."
        }
        ErrorCode.DUCHY_NOT_FOUND -> {
          put(
            "duchy",
            DuchyKey(checkNotNull(errorInfo.metadataMap["external_duchy_id"]).toString()).toName(),
          )
          errorMessage = "Duchy ${get("duchy")} not found."
        }
        // TODO{@jcorilla}: Look to differentiate certificate types
        ErrorCode.CERTIFICATE_NOT_FOUND -> {
          put(
            "certificate",
            checkNotNull(errorInfo.metadataMap["external_certificate_id"]).toString(),
          )
          errorMessage = "Certificate not found."
        }
        ErrorCode.CERTIFICATE_IS_INVALID -> {
          errorMessage = "Certificate is invalid."
        }
        ErrorCode.DUCHY_NOT_ACTIVE -> {
          put(
            "duchy",
            DuchyKey(checkNotNull(errorInfo.metadataMap["external_duchy_id"]).toString()).toName(),
          )
          errorMessage = "Duchy ${get("duchy")} is not active."
        }
        ErrorCode.MEASUREMENT_STATE_ILLEGAL -> {
          put(
            "measurement",
            MeasurementKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                ),
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_id"]).toLong()
                ),
              )
              .toName(),
          )
          put("state", checkNotNull(errorInfo.metadataMap["measurement_state"]).toString())
          errorMessage = "Measurement ${get("measurement")} is in illegal state: ${get("state")}"
        }
        // TODO{@jcorilla}: Populate metadata using subsequent error codes
        ErrorCode.MODEL_PROVIDER_NOT_FOUND -> {
          errorMessage = "ModelProvider not found."
        }
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS -> {
          errorMessage = "Certificate with the subject key identifier (SKID) already exists."
        }
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL -> {
          errorMessage = "Certificate is in wrong State."
        }
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL -> {
          errorMessage = "ComputationParticipant not in CREATED state."
        }
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND -> {
          errorMessage = "ComputationParticipant not found."
        }
        ErrorCode.REQUISITION_NOT_FOUND -> {
          errorMessage = "Requisition not found."
        }
        ErrorCode.REQUISITION_STATE_ILLEGAL -> {
          errorMessage = "Requisition state illegal."
        }
        ErrorCode.ACCOUNT_NOT_FOUND -> {
          errorMessage = "Account not found."
        }
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY -> {
          errorMessage = "Duplicated account identity."
        }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL -> {
          errorMessage = "Account activation state illegal."
        }
        ErrorCode.PERMISSION_DENIED -> {
          errorMessage = "Permission Denied."
        }
        ErrorCode.API_KEY_NOT_FOUND -> {
          errorMessage = "ApiKey not found."
        }
        ErrorCode.EVENT_GROUP_NOT_FOUND -> {
          errorMessage = "EventGroup not found."
        }
        ErrorCode.EVENT_GROUP_INVALID_ARGS -> {
          errorMessage = "EventGroup invalid arguments."
        }
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND -> {
          errorMessage = "EventGroup metadata descriptor not found."
        }
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_ALREADY_EXISTS_WITH_TYPE -> {
          errorMessage = "EventGroupMetadataDescriptor with same type already exists."
        }
        ErrorCode.RECURRING_EXCHANGE_NOT_FOUND -> {
          errorMessage = "RecurringExchange not found."
        }
        ErrorCode.EXCHANGE_STEP_NOT_FOUND -> {
          errorMessage = "ExchangeStep not found."
        }
        ErrorCode.EXCHANGE_STEP_ATTEMPT_NOT_FOUND -> {
          errorMessage = "ExchangeStepAttempt not found."
        }
        ErrorCode.EVENT_GROUP_STATE_ILLEGAL -> {
          errorMessage = "EventGroup not found."
        }
        ErrorCode.MEASUREMENT_ETAG_MISMATCH -> {
          errorMessage = "Measurement is inconsistent with initial state."
        }
        ErrorCode.MODEL_SUITE_NOT_FOUND -> {
          errorMessage = "ModelSuite not found."
        }
        ErrorCode.MODEL_LINE_NOT_FOUND -> {
          errorMessage = "ModelLine not found."
        }
        ErrorCode.MODEL_LINE_TYPE_ILLEGAL -> {
          errorMessage = "ModelLine type illegal."
        }
        ErrorCode.MODEL_LINE_INVALID_ARGS -> {
          errorMessage = "ModelLine invalid active times."
        }
        ErrorCode.MODEL_OUTAGE_NOT_FOUND -> {
          errorMessage = "ModelOutage not found."
        }
        ErrorCode.MODEL_OUTAGE_INVALID_ARGS -> {
          errorMessage = "ModelOutage invalid outage intervals."
        }
        ErrorCode.MODEL_OUTAGE_STATE_ILLEGAL -> {
          errorMessage = "ModelOutage not found."
        }
        ErrorCode.MODEL_SHARD_NOT_FOUND -> {
          errorMessage = "ModelShard not found."
        }
        ErrorCode.MODEL_RELEASE_NOT_FOUND -> {
          errorMessage = "ModelRelease not found."
        }
        ErrorCode.MODEL_ROLLOUT_INVALID_ARGS -> {
          errorMessage = "ModelRollout invalid rollout period times."
        }
        ErrorCode.MODEL_ROLLOUT_NOT_FOUND -> {
          errorMessage = "ModelRollout not found."
        }
        ErrorCode.EXCHANGE_NOT_FOUND -> {
          errorMessage = "Exchange not found."
        }
        ErrorCode.MODEL_SHARD_INVALID_ARGS -> {
          errorMessage = "ModelShard invalid arguments."
        }
        ErrorCode.POPULATION_NOT_FOUND -> {
          errorMessage = "Population not found."
        }
        ErrorCode.UNKNOWN_ERROR -> {
          errorMessage = "Unknown exception."
        }
        ErrorCode.UNRECOGNIZED -> {
          errorMessage = "Unrecognized exception."
        }
      }
    }

  val statusProto = status {
    code = this@toExternalStatusRuntimeException.code.value()
    message = errorMessage
    details += ProtoAny.pack(errorInfo { metadata.putAll(metadataMap) })
  }
  return StatusProto.toStatusRuntimeException(statusProto)
}
