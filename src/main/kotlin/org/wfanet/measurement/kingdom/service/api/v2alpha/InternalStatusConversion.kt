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
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.ModelProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.PopulationKey
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
          val measurementName =
            MeasurementKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                ),
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_id"]).toLong()
                ),
              )
              .toName()
          put("measurement", measurementName)
          errorMessage = "Measurement $measurementName not found"
        }
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND -> {
          val measurementConsumerName =
            MeasurementConsumerKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                )
              )
              .toName()
          put("measurement_consumer", measurementConsumerName)
          errorMessage = "MeasurementConsumer $measurementConsumerName not found."
        }
        ErrorCode.DATA_PROVIDER_NOT_FOUND -> {
          val dataProviderName =
            DataProviderKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_data_provider_id"]).toLong()
                )
              )
              .toName()
          put("data_provider", dataProviderName)
          errorMessage = "DataProvider $dataProviderName not found."
        }
        ErrorCode.DUCHY_NOT_FOUND -> {
          val duchyName =
            DuchyKey(checkNotNull(errorInfo.metadataMap["external_duchy_id"]).toString()).toName()
          put("duchy", duchyName)
          errorMessage = "Duchy $duchyName not found."
        }
        ErrorCode.CERTIFICATE_NOT_FOUND -> {
          val certificateApiId =
            externalIdToApiId(
              checkNotNull(errorInfo.metadataMap["external_certificate_id"]).toLong()
            )
          if (errorInfo.metadataMap.containsKey("external_data_provider_id")) {
            val dataProviderCertificateName =
              DataProviderCertificateKey(
                  externalIdToApiId(
                    checkNotNull(errorInfo.metadataMap["external_data_provider_id"]).toLong()
                  ),
                  certificateApiId,
                )
                .toName()
            put("data_provider_certificate", dataProviderCertificateName)
            errorMessage = "DataProviderCertificate $dataProviderCertificateName not found."
          } else if (errorInfo.metadataMap.containsKey("external_measurement_consumer_id")) {
            val measurementConsumerCertificateName =
              MeasurementConsumerCertificateKey(
                  externalIdToApiId(
                    checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                  ),
                  certificateApiId,
                )
                .toName()
            put("measurement_consumer_certificate", measurementConsumerCertificateName)
            errorMessage =
              "MeasurementConsumerCertificate $measurementConsumerCertificateName not found."
          } else if (errorInfo.metadataMap.containsKey("external_duchy_id")) {
            val duchyCertificateName =
              DuchyCertificateKey(
                  checkNotNull(errorInfo.metadataMap["external_duchy_id"]).toString(),
                  certificateApiId,
                )
                .toName()
            put("duchy_certificate", duchyCertificateName)
            errorMessage = "DuchyCertificate $duchyCertificateName not found."
          } else if (errorInfo.metadataMap.containsKey("external_model_provider_id")) {
            val modelProviderCertificateName =
              ModelProviderCertificateKey(
                  externalIdToApiId(
                    checkNotNull(errorInfo.metadataMap["external_model_provider_id"]).toLong()
                  ),
                  certificateApiId,
                )
                .toName()
            put("model_provider_certificate", modelProviderCertificateName)
            errorMessage = "ModelProviderCertificate $modelProviderCertificateName not found."
          } else {
            put("external_certificate_id", certificateApiId)
            errorMessage = "Certificate not found."
          }
        }
        ErrorCode.CERTIFICATE_IS_INVALID -> {
          errorMessage = "Certificate is invalid."
        }
        ErrorCode.DUCHY_NOT_ACTIVE -> {
          val duchyName =
            DuchyKey(checkNotNull(errorInfo.metadataMap["external_duchy_id"]).toString()).toName()

          put("duchy", duchyName)
          errorMessage = "Duchy $duchyName is not active."
        }
        ErrorCode.MEASUREMENT_STATE_ILLEGAL -> {
          val measurementName =
            MeasurementKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_consumer_id"]).toLong()
                ),
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_measurement_id"]).toLong()
                ),
              )
              .toName()
          val measurementState = checkNotNull(errorInfo.metadataMap["measurement_state"]).toString()
          put("measurement", measurementName)
          put("state", measurementState)
          errorMessage = "Measurement $measurementName is in illegal state: $measurementState"
        }
        // TODO{@jcorilla}: Populate metadata using subsequent error codes
        ErrorCode.MODEL_PROVIDER_NOT_FOUND -> {
          val modelProviderName =
            ModelProviderKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_model_provider_id"]).toLong()
                )
              )
              .toName()
          put("model_provider", modelProviderName)
          errorMessage = "ModelProvider $modelProviderName not found."
        }
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS -> {
          errorMessage = "Certificate with the subject key identifier (SKID) already exists."
        }
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL -> {
          val certificateApiId =
            externalIdToApiId(
              checkNotNull(errorInfo.metadataMap["external_certificate_id"]).toLong()
            )
          val certificateRevocationState =
            checkNotNull(errorInfo.metadataMap["certificate_revocation_state"]).toString()
          put("external_certificate_id", certificateApiId)
          put("certification_revocation_state", certificateRevocationState)
          errorMessage = "Certificate is in illegal revocation state: $certificateRevocationState."
        }
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL -> {
          errorMessage = "ComputationParticipant state illegal."
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
          val accountName =
            AccountKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_account_id"]).toLong()
                )
              )
              .toName()
          put("account", accountName)
          errorMessage = "Account $accountName not found."
        }
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY -> {
          val accountName =
            AccountKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_account_id"]).toLong()
                )
              )
              .toName()
          val issuer = checkNotNull(errorInfo.metadataMap["issuer"])
          val subject = checkNotNull(errorInfo.metadataMap["subject"])
          put("account", accountName)
          put("issuer", issuer)
          put("subject", subject)
          errorMessage =
            "Account $accountName with issuer: $issuer and subject: $subject pair already exists."
        }
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL -> {
          val accountName =
            AccountKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_account_id"]).toLong()
                )
              )
              .toName()
          val accountActivationState =
            checkNotNull(errorInfo.metadataMap["account_activation_state"]).toString()
          put("account", accountName)
          put("account_activation_state", accountActivationState)
          errorMessage =
            "Account $accountName is in illegal activation state: $accountActivationState."
        }
        ErrorCode.PERMISSION_DENIED -> {
          errorMessage = "Permission Denied."
        }
        ErrorCode.API_KEY_NOT_FOUND -> {
          val apiKeyApiId =
            externalIdToApiId(checkNotNull(errorInfo.metadataMap["external_api_key_id"]).toLong())

          put("external_api_key_id", apiKeyApiId)
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
          val populationName =
            PopulationKey(
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_data_provider_id"]).toLong()
                ),
                externalIdToApiId(
                  checkNotNull(errorInfo.metadataMap["external_population_id"]).toLong()
                ),
              )
              .toName()
          put("population", populationName)
          errorMessage = "Population $populationName not found."
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
