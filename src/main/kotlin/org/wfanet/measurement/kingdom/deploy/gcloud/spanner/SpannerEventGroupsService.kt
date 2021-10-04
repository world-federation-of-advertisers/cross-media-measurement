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
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroup

class SpannerEventGroupsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : EventGroupsCoroutineImplBase() {

  override suspend fun createEventGroup(request: EventGroup): EventGroup {
    try {
      return CreateEventGroup(request).execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND ->
          failGrpc(Status.INVALID_ARGUMENT) { "MeasurementConsumer not found" }
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "DataProvider not found" }
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL -> throw e
      }
    }
  }

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    return EventGroupReader().readByExternalId(client.singleUse(), request.externalEventGroupId)
      ?: failGrpc(Status.NOT_FOUND) { "EventGroup not found" }
  }

  override fun streamEventGroups(request: StreamEventGroupsRequest): Flow<EventGroup> {
    return StreamEventGroups(request.filter, request.limit).execute(client.singleUse())
  }
}
