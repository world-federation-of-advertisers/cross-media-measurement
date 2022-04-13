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

class ErrorContext {
  var state: Int? = null
  var externalAccountId: Long? = null
  var externalMeasurementConsumerId: Long? = null
  var internalMeasurementConsumerId: Long? = null
  var externalMeasurementId: Long? = null
  var internalMeasurementId: Long? = null
  var externalApiKeyId: Long? = null
  var externalDataProviderId: Long? = null
  var externalEventGroupId: Long? = null
  var externalDuchyId: String? = null
  var internalDuchyId: Long? = null
  var externalComputationId: Long? = null
  var externalRequisitionId: Long? = null
  var parentId: Long? = null
  var externalCertificateId: Long? = null
  var externalModelProviderId: Long? = null

  private fun addMapItem(map: MutableMap<String, String>, key: String, value: String?) {
    if (!value.isNullOrEmpty()) {
      map[key] = value
    }
  }

  fun toMap(): Map<String, String> {
    val map = mutableMapOf<String, String>()
    addMapItem(map, "state", state?.toString())
    addMapItem(map, "external_account_id", externalAccountId?.toString())
    addMapItem(map, "external_measurement_consumer_id", externalMeasurementConsumerId?.toString())
    addMapItem(map, "internal_measurement_consumer_id", internalMeasurementConsumerId?.toString())
    addMapItem(map, "external_measurement_id", externalMeasurementId?.toString())
    addMapItem(map, "internal_measurement_id", internalMeasurementId?.toString())
    addMapItem(map, "external_api_key_id", externalApiKeyId?.toString())
    addMapItem(map, "external_data_provider_id", externalDataProviderId?.toString())
    addMapItem(map, "external_event_group_id", externalEventGroupId?.toString())
    addMapItem(map, "external_duchy_id", externalDuchyId)
    addMapItem(map, "internal_duchy_id", internalDuchyId?.toString())
    addMapItem(map, "external_computation_id", externalComputationId?.toString())
    addMapItem(map, "external_requisition_id", externalRequisitionId?.toString())
    addMapItem(map, "parent_id", parentId?.toString())
    addMapItem(map, "external_certificate_id", externalCertificateId?.toString())
    addMapItem(map, "external_model_provider_id", externalModelProviderId?.toString())

    return map
  }
}

open class KingdomInternalException : Exception {
  val code: ErrorCode
  val context: ErrorContext

  constructor(code: ErrorCode, context: ErrorContext = ErrorContext()) : super() {
    this.code = code
    this.context = context
  }

  constructor(
    code: ErrorCode,
    context: ErrorContext = ErrorContext(),
    buildMessage: () -> String
  ) : super(buildMessage()) {
    this.code = code
    this.context = context
  }

  fun throwStatusRuntimeException(
    status: Status = Status.INVALID_ARGUMENT,
    provideDescription: () -> String,
  ): Nothing = throwStatusRuntimeException(status, code, context, provideDescription)
}

fun throwStatusRuntimeException(
  status: Status = Status.INVALID_ARGUMENT,
  code: ErrorCode,
  context: ErrorContext,
  provideDescription: () -> String,
): Nothing {
  val info = errorInfo {
    reason = code.toString()
    domain = ErrorInfo::class.qualifiedName.toString()
    metadata.putAll(context.toMap())
  }

  val metadata = Metadata()
  metadata.put(ProtoUtils.keyForProto(info), info)

  throw status.withDescription(provideDescription()).asRuntimeException(metadata)
}

fun StatusRuntimeException.getErrorInfo(): ErrorInfo? {
  val key = ProtoUtils.keyForProto(ErrorInfo.getDefaultInstance())
  return trailers?.get(key)
}

class MeasurementConsumerNotFound(
  externalMeasurementConsumerId: Long,
  provideDescription: () -> String = { "MeasurementConsumer not found" }
) :
  KingdomInternalException(
    ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
    ErrorContext(),
    provideDescription
  ) {
  init {
    context.externalMeasurementConsumerId = externalMeasurementConsumerId
  }
}

class DataProviderNotFound(
  externalDataProviderId: Long,
  provideDescription: () -> String = { "DataProvider not found" }
) :
  KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.externalDataProviderId = externalDataProviderId
  }
}

class ModelProviderNotFound(
  externalModelProviderId: Long,
  provideDescription: () -> String = { "ModelProvider not found" }
) :
  KingdomInternalException(ErrorCode.MODEL_PROVIDER_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.externalModelProviderId = externalModelProviderId
  }
}

class DuchyNotFound(
  externalDuchyId: String,
  provideDescription: () -> String = { "Duchy not found" }
) : KingdomInternalException(ErrorCode.DUCHY_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.externalDuchyId = externalDuchyId
  }
}

class MeasurementNotFound(provideDescription: () -> String) :
  KingdomInternalException(ErrorCode.MEASUREMENT_NOT_FOUND, ErrorContext(), provideDescription) {
  constructor(
    externalMeasurementConsumerId: Long,
    externalMeasurementId: Long,
    provideDescription: () -> String = { "Measurement not found" }
  ) : this(provideDescription) {
    context.externalMeasurementConsumerId = externalMeasurementConsumerId
    context.externalMeasurementId = externalMeasurementId
  }
  constructor(
    externalComputationId: Long,
    provideDescription: () -> String = { "Measurement not found" }
  ) : this(provideDescription) {
    context.externalComputationId = externalComputationId
  }
}

class CertificateNotFound(provideDescription: () -> String) :
  KingdomInternalException(ErrorCode.CERTIFICATE_NOT_FOUND, ErrorContext(), provideDescription) {
  constructor(
    parentId: Long,
    externalCertificateId: Long,
    provideDescription: () -> String = { "Certificate not found" }
  ) : this(provideDescription) {
    context.parentId = parentId
    context.externalCertificateId = externalCertificateId
  }
}

class ComputationParticipantNotFound(provideDescription: () -> String) :
  KingdomInternalException(
    ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
    ErrorContext(),
    provideDescription
  ) {
  constructor(
    externalComputationId: Long,
    externalDuchyId: String,
    provideDescription: () -> String = { "ComputationParticipant not found" }
  ) : this(provideDescription) {
    context.externalComputationId = externalComputationId
    context.externalDuchyId = externalDuchyId
  }

  constructor(
    internalMeasurementConsumerId: Long,
    internalMeasurementId: Long,
    internalDuchyId: Long,
    provideDescription: () -> String = { "ComputationParticipant not found" }
  ) : this(provideDescription) {
    context.internalMeasurementConsumerId = internalMeasurementConsumerId
    context.internalMeasurementId = internalMeasurementId
    context.internalDuchyId = internalDuchyId
  }
}

class RequisitionNotFound(provideDescription: () -> String) :
  KingdomInternalException(ErrorCode.REQUISITION_NOT_FOUND, ErrorContext(), provideDescription) {
  constructor(
    externalComputationId: Long,
    externalRequisitionId: Long,
    provideDescription: () -> String = { "Requisition not found" }
  ) : this(provideDescription) {
    context.externalComputationId = externalComputationId
    context.externalRequisitionId = externalRequisitionId
  }
}

class AccountNotFound(
  externalAccountId: Long,
  provideDescription: () -> String = { "Account not found" }
) : KingdomInternalException(ErrorCode.ACCOUNT_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.externalAccountId = externalAccountId
  }
}

class ApiKeyNotFound(
  externalApiKeyId: Long,
  provideDescription: () -> String = { "ApiKey not found" }
) : KingdomInternalException(ErrorCode.API_KEY_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.externalApiKeyId = externalApiKeyId
  }
}

class EventGroupNotFound(
  externalDataProviderId: Long,
  externalEventGroupId: Long,
  provideDescription: () -> String = { "EventGroup not found" }
) : KingdomInternalException(ErrorCode.EVENT_GROUP_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.externalDataProviderId = externalDataProviderId
    context.externalEventGroupId = externalEventGroupId
  }
}

class MeasurementStateIllegal(
  state: Int,
  provideDescription: () -> String = { "Measurement state illegal" }
) :
  KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, ErrorContext(), provideDescription) {
  init {
    context.state = state
  }
}

class CertificateRevocationStateIllegal(
  state: Int,
  provideDescription: () -> String = { "CertificateRevocation state illegal" }
) :
  KingdomInternalException(
    ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
    ErrorContext(),
    provideDescription
  ) {
  init {
    context.state = state
  }
}

class ComputationParticipantStateIllegal(
  state: Int,
  provideDescription: () -> String = { "ComputationParticipant state illegal" }
) :
  KingdomInternalException(
    ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
    ErrorContext(),
    provideDescription
  ) {
  init {
    context.state = state
  }
}

class RequisitionStateIllegal(
  state: Int,
  provideDescription: () -> String = { "Requisition state illegal" }
) :
  KingdomInternalException(
    ErrorCode.REQUISITION_STATE_ILLEGAL,
    ErrorContext(),
    provideDescription
  ) {
  init {
    context.state = state
  }
}

class AccountActivationStateIllegal(
  state: Int,
  provideDescription: () -> String = { "AccountActivation state illegal" }
) :
  KingdomInternalException(
    ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
    ErrorContext(),
    provideDescription
  ) {
  init {
    context.state = state
  }
}
