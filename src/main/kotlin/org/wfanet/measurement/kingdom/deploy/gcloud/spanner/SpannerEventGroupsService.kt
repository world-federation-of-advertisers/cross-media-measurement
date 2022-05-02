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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateEventGroup

class SpannerEventGroupsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : EventGroupsCoroutineImplBase() {

  override suspend fun createEventGroup(request: EventGroup): EventGroup {
    try {
      return CreateEventGroup(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "MeasurementConsumer not found" }
        ErrorCode.DATA_PROVIDER_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
        ErrorCode.CERTIFICATE_IS_INVALID ->
          failGrpc(Status.FAILED_PRECONDITION) { "MeasurementConsumer certificate is invalid" }
        ErrorCode.CERTIFICATE_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "MeasurementConsumer certificate not found" }
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
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun updateEventGroup(request: UpdateEventGroupRequest): EventGroup {
    grpcRequire(request.eventGroup.externalDataProviderId > 0L) {
      "ExternalDataProviderId unspecified"
    }
    grpcRequire(request.eventGroup.externalEventGroupId > 0L) { "ExternalEventGroupId unspecified" }
    try {
      return UpdateEventGroup(request.eventGroup).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        ErrorCode.EVENT_GROUP_INVALID_ARGS ->
          failGrpc(Status.INVALID_ARGUMENT) { "EventGroup modification param is invalid" }
        ErrorCode.CERTIFICATE_IS_INVALID ->
          failGrpc(Status.FAILED_PRECONDITION) { "MeasurementConsumer certificate is invalid" }
        ErrorCode.CERTIFICATE_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "MeasurementConsumer certificate not found" }
        ErrorCode.EVENT_GROUP_NOT_FOUND -> failGrpc(Status.NOT_FOUND) { "EventGroup not found" }
        ErrorCode.EVENT_GROUP_METADATA_DESCRIPTOR_NOT_FOUND,
        ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
        ErrorCode.DATA_PROVIDER_NOT_FOUND,
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
        ErrorCode.UNKNOWN_ERROR,
        ErrorCode.UNRECOGNIZED -> throw e
      }
    }
  }

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    return EventGroupReader()
      .readByExternalIds(
        client.singleUse(),
        request.externalDataProviderId,
        request.externalEventGroupId
      )
      ?.eventGroup
      ?: failGrpc(Status.NOT_FOUND) { "EventGroup not found" }
  }

  override fun streamEventGroups(request: StreamEventGroupsRequest): Flow<EventGroup> {
    return StreamEventGroups(request.filter, request.limit).execute(client.singleUse()).map {
      it.eventGroup
    }
  }
}
