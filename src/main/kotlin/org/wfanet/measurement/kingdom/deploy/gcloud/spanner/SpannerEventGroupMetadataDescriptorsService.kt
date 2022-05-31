// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroupMetadataDescriptors
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroupMetadataDescriptor
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateEventGroupMetadataDescriptor

class SpannerEventGroupMetadataDescriptorsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : EventGroupMetadataDescriptorsCoroutineImplBase() {
  override suspend fun createEventGroupMetadataDescriptor(
    request: EventGroupMetadataDescriptor
  ): EventGroupMetadataDescriptor {
    try {
      return CreateEventGroupMetadataDescriptor(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.DATA_PROVIDER_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.CERTIFICATE_NOT_FOUND,
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun getEventGroupMetadataDescriptor(
    request: GetEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    return EventGroupMetadataDescriptorReader()
      .readByExternalIds(
        client.singleUse(),
        request.externalDataProviderId,
        request.externalEventGroupMetadataDescriptorId
      )
      ?.eventGroupMetadataDescriptor
      ?: failGrpc(Status.NOT_FOUND) { "EventGroupMetadataDescriptor not found" }
  }

  override suspend fun updateEventGroupMetadataDescriptor(
    request: UpdateEventGroupMetadataDescriptorRequest
  ): EventGroupMetadataDescriptor {
    grpcRequire(request.eventGroupMetadataDescriptor.externalDataProviderId > 0L) {
      "ExternalDataProviderId unspecified"
    }
    grpcRequire(request.eventGroupMetadataDescriptor.externalEventGroupMetadataDescriptorId > 0L) {
      "ExternalEventGroupMetadataDescriptorId unspecified"
    }
    try {
      return UpdateEventGroupMetadataDescriptor(request.eventGroupMetadataDescriptor)
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "EventGroupMetadataDescriptor not found" }
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.CERTIFICATE_NOT_FOUND,
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND,
        ErrorCode.REQUISITION_NOT_FOUND,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override fun streamEventGroupMetadataDescriptors(
    request: StreamEventGroupMetadataDescriptorsRequest
  ): Flow<EventGroupMetadataDescriptor> {
    return StreamEventGroupMetadataDescriptors(request.filter, request.limit)
      .execute(client.singleUse())
      .map { it.eventGroupMetadataDescriptor }
  }
}
