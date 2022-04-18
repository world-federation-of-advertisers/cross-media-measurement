// Copyright 2021 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptor
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupMetadataDescriptorReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroupMetadataDescriptor
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateEventGroupMetadataDescriptor

class SpannerEventGroupMetadataDescriptorsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : EventGroupMetadataDescriptorsCoroutineImplBase() {
  override suspend fun createEventGroupMetadataDescriptor(
    eventGroupMetadataDescriptor: EventGroupMetadataDescriptor
  ): EventGroupMetadataDescriptor {
    try {
      return CreateEventGroupMetadataDescriptor(eventGroupMetadataDescriptor)
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.DATA_PROVIDER_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.CERTIFICATE_IS_INVALID,
        ErrorCode.MEASUREMENT_CONSUMER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DUCHY_CERTIFICATE_NOT_FOUND,
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_MEASUREMENT_CONSUMER,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_MEASUREMENT,
        ErrorCode.REQUISITION_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.REQUISITION_NOT_FOUND_BY_DATA_PROVIDER,
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
        ErrorCode.MEASUREMENT_CONSUMER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_CERTIFICATE_NOT_FOUND,
        ErrorCode.DUCHY_CERTIFICATE_NOT_FOUND,
        ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL,
        ErrorCode.DUPLICATE_ACCOUNT_IDENTITY,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.API_KEY_NOT_FOUND,
        ErrorCode.PERMISSION_DENIED,
        ErrorCode.MODEL_PROVIDER_NOT_FOUND,
        ErrorCode.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        ErrorCode.DUCHY_NOT_FOUND,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.MEASUREMENT_NOT_FOUND_BY_MEASUREMENT_CONSUMER,
        ErrorCode.MEASUREMENT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_MEASUREMENT,
        ErrorCode.COMPUTATION_PARTICIPANT_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.REQUISITION_NOT_FOUND_BY_DATA_PROVIDER,
        ErrorCode.REQUISITION_NOT_FOUND_BY_COMPUTATION,
        ErrorCode.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        ErrorCode.REQUISITION_STATE_ILLEGAL,
        ErrorCode.EVENT_GROUP_INVALID_ARGS,
        ErrorCode.EVENT_GROUP_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }
}
