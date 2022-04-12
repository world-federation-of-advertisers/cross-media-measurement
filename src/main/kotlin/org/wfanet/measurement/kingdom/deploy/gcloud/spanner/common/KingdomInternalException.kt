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

/* Throw internal exceptions with reserved parameters

Throw internal exception:
throw MeasurementConsumerNotFoundError(id="123") { "measurement_consumer not existing" }

Catch internal exception and throw Grpc runtime exception to the client:
catch(e: KingdomInternalException) {
  when(e) {
    ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND -> {
      val externalMeasurementConsumerId = e.context.externalMeasurementConsumerId ?: 0L
      e.throwRuntimeException(Status.FAILED_PRECONDITION) { "MeasurementConsumer not found" }
    }
    else -> {}
  }
}

The client receive the Grpc runtime exception and check reason and context:
catch(e: StatusRuntimeException) {
   val info = e.getErrorInfo()
   if(info.notNull() && info.reason = MEASUREMENT_CONSUMER_NOT_FOUND.getName()) {
       val externalMeasurementConsumerId = info.metadata.getOrDefault("externalMeasurementConsumerId", 0L)
       blame(measurementConsumerId)
   }
}
 */

class ErrorContext {
  var externalAccountId: Long? = null
  var accountActivationState: Int? = null
  var externalMeasurementConsumerId: Long? = null
  var externalMeasurementConsumerCertificateId: Long? = null
  var externalMeasurementId: Long? = null
  var providedMeasurementId: String? = null
  var measurementState: Int? = null
  var externalApiKeyId: Long? = null
  var externalDataProviderId: Long? = null
  var externalDataProviderCertificateId: Long? = null
  var externalEventGroupId: Long? = null
  var providedEventGroupId: String? = null
  var externalEventGroupMetadataDescriptorId: Long? = null
  var externalDuchyId: String? = null
  var internalDuchyId: Long? = null
  var externalComputationId: Long? = null
  var computationState: Int? = null
  var externalRequisitionId: Long? = null
  var requisitionState: Int? = null
  var externalFulfillingDuchyId: String? = null
  var externalCertificateId: Long? = null
  var certificationRevocationState: Int? = null
  var externalRecurringExchangeId: Long? = null
  var externalModelProviderId: Long? = null
  var externalProtocolConfigId: String? = null

  private fun addMapItem(map: MutableMap<String, String>, key: String, value: String?) {
    if (!value.isNullOrEmpty()) {
      map[key] = value
    }
  }

  fun toMap(): Map<String, String> {
    val map = mutableMapOf<String, String>()
    addMapItem(map, "externalAccountId", externalAccountId?.toString())
    addMapItem(map, "accountActivationState", accountActivationState?.toString())
    addMapItem(map, "externalMeasurementConsumerId", externalMeasurementConsumerId?.toString())
    addMapItem(
      map,
      "externalMeasurementConsumerCertificateId",
      externalMeasurementConsumerCertificateId?.toString()
    )
    addMapItem(map, "externalMeasurementId", externalMeasurementId?.toString())
    addMapItem(map, "providedMeasurementId", providedMeasurementId)
    addMapItem(map, "measurementState", measurementState?.toString())
    addMapItem(map, "externalApiKeyId", externalApiKeyId?.toString())
    addMapItem(map, "externalDataProviderId", externalDataProviderId?.toString())
    addMapItem(
      map,
      "externalDataProviderCertificateId",
      externalDataProviderCertificateId?.toString()
    )
    addMapItem(map, "externalEventGroupId", externalEventGroupId?.toString())
    addMapItem(map, "providedEventGroupId", providedEventGroupId)
    addMapItem(
      map,
      "externalEventGroupMetadataDescriptorId",
      externalEventGroupMetadataDescriptorId?.toString()
    )
    addMapItem(map, "externalDuchyId", externalDuchyId)
    addMapItem(map, "internalDuchyId", internalDuchyId?.toString())
    addMapItem(map, "externalComputationId", externalComputationId?.toString())
    addMapItem(map, "computationState", computationState?.toString())
    addMapItem(map, "externalRequisitionId", externalRequisitionId?.toString())
    addMapItem(map, "requisitionState", requisitionState?.toString())
    addMapItem(map, "externalFulfillingDuchyId", externalFulfillingDuchyId)
    addMapItem(map, "externalCertificateId", externalCertificateId?.toString())
    addMapItem(map, "certificationRevocationState", externalCertificateId?.toString())
    addMapItem(map, "externalRecurringExchangeId", externalRecurringExchangeId?.toString())
    addMapItem(map, "externalModelProviderId", externalModelProviderId?.toString())
    addMapItem(map, "externalProtocolConfigId", externalProtocolConfigId)

    return map
  }
}

open class KingdomInternalException : Exception {
  val code: ErrorCode
  val context = ErrorContext()

  constructor(code: ErrorCode) : super() {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
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

class MeasurementConsumerNotFoundError(
  externalMeasurementConsumerId: Long,
  provideDescription: () -> String
) : KingdomInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND, provideDescription) {
  init {
    context.externalMeasurementConsumerId = externalMeasurementConsumerId
  }
}

class DataProviderNotFoundError(
  externalDataProviderId: Long,
  provideDescription: () -> String = { "" }
) : KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, provideDescription) {
  init {
    context.externalDataProviderId = externalDataProviderId
  }
}

class MeasurementStateIllegalError(
  externalMeasurementId: Long,
  measurementState: Int,
  provideDescription: () -> String
) : KingdomInternalException(ErrorCode.DATA_PROVIDER_NOT_FOUND, provideDescription) {
  init {
    context.externalMeasurementId = externalMeasurementId
    context.measurementState = measurementState
  }
}
