/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor as CmmsEventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as CmmsEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.reporting.v2alpha.BatchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.reporting.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.reporting.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.reporting.v2alpha.batchGetEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.reporting.v2alpha.eventGroupMetadataDescriptor

class EventGroupMetadataDescriptorsService(
  private val cmmsEventGroupMetadataDescriptorsStub: CmmsEventGroupMetadataDescriptorsCoroutineStub,
) : EventGroupMetadataDescriptorsCoroutineImplBase() {
  override suspend fun getEventGroupMetadataDescriptor(
    request: GetEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {}
    }

    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequireNotNull(EventGroupMetadataDescriptorKey.fromName(request.name)) {
      "Resource name is unspecified or invalid"
    }

    val cmmsEventGroupMetadataDescriptor =
      try {
        cmmsEventGroupMetadataDescriptorsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .getEventGroupMetadataDescriptor(
            getEventGroupMetadataDescriptorRequest { name = request.name }
          )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .asRuntimeException()
      }

    return cmmsEventGroupMetadataDescriptor.toEventGroupMetadataDescriptor()
  }
  override suspend fun batchGetEventGroupMetadataDescriptors(
    request: BatchGetEventGroupMetadataDescriptorsRequest
  ): BatchGetEventGroupMetadataDescriptorsResponse {
    val principal: ReportingPrincipal = principalFromCurrentContext
    when (principal) {
      is MeasurementConsumerPrincipal -> {}
    }

    val apiAuthenticationKey: String = principal.config.apiKey

    grpcRequire(request.namesCount > 0) {
      "Must include at least one EventGroupMetadataDescriptor resource name"
    }

    val cmmsListEventGroupMetadataDescriptorsResponse =
      try {
        cmmsEventGroupMetadataDescriptorsStub
          .withAuthenticationKey(apiAuthenticationKey)
          .batchGetEventGroupMetadataDescriptors(
            batchGetEventGroupMetadataDescriptorsRequest {
              parent = DataProviderKey(ResourceKey.WILDCARD_ID).toName()
              names += request.namesList
            }
          )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .asRuntimeException()
      }
    val cmmsEventGroupMetadataDescriptors =
      cmmsListEventGroupMetadataDescriptorsResponse.eventGroupMetadataDescriptorsList

    val eventGroupMetadataDescriptors =
      cmmsEventGroupMetadataDescriptors.map { it.toEventGroupMetadataDescriptor() }

    return batchGetEventGroupMetadataDescriptorsResponse {
      this.eventGroupMetadataDescriptors += eventGroupMetadataDescriptors
    }
  }

  private fun CmmsEventGroupMetadataDescriptor.toEventGroupMetadataDescriptor():
    EventGroupMetadataDescriptor {
    val source = this
    return eventGroupMetadataDescriptor {
      name = source.name
      descriptorSet = source.descriptorSet
    }
  }
}
