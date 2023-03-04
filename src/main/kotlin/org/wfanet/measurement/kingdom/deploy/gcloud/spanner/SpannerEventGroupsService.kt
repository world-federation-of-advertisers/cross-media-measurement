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
import org.wfanet.measurement.internal.kingdom.DeleteEventGroupRequest
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.internal.kingdom.StreamEventGroupsRequest
import org.wfanet.measurement.internal.kingdom.UpdateEventGroupRequest
import org.wfanet.measurement.internal.kingdom.eventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.CertificateIsInvalidException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.EventGroupStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerCertificateNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamEventGroups
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteEventGroup
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.UpdateEventGroup

class SpannerEventGroupsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : EventGroupsCoroutineImplBase() {

  override suspend fun createEventGroup(request: EventGroup): EventGroup {
    try {
      return CreateEventGroup(request).execute(client, idGenerator)
    } catch (e: MeasurementConsumerNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "MeasurementConsumer not found." }
    } catch (e: DataProviderNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "DataProvider not found." }
    } catch (e: CertificateIsInvalidException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "MeasurementConsumer's Certificate is invalid."
      }
    } catch (e: MeasurementConsumerCertificateNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "MeasurementConsumer's Certificate not found."
      }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override suspend fun updateEventGroup(request: UpdateEventGroupRequest): EventGroup {
    grpcRequire(request.eventGroup.externalDataProviderId > 0L) {
      "ExternalDataProviderId unspecified"
    }
    grpcRequire(request.eventGroup.externalEventGroupId > 0L) { "ExternalEventGroupId unspecified" }
    try {
      return UpdateEventGroup(request.eventGroup).execute(client, idGenerator)
    } catch (e: EventGroupInvalidArgsException) {
      e.throwStatusRuntimeException(Status.INVALID_ARGUMENT) {
        "EventGroup modification param is invalid."
      }
    } catch (e: CertificateIsInvalidException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "MeasurementConsumer's Certificate is invalid."
      }
    } catch (e: MeasurementConsumerCertificateNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "MeasurementConsumer's Certificate not found."
      }
    } catch (e: EventGroupNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "EventGroup not found." }
    } catch (e: EventGroupStateIllegalException) {
      when (e.state) {
        EventGroup.State.DELETED ->
          e.throwStatusRuntimeException(Status.NOT_FOUND) { "EventGroup state is DELETED." }
        EventGroup.State.ACTIVE,
        EventGroup.State.STATE_UNSPECIFIED,
        EventGroup.State.UNRECOGNIZED ->
          e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
      }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    return EventGroupReader()
      .readByExternalIds(
        client.singleUse(),
        request.externalDataProviderId,
        request.externalEventGroupId,
      )
      ?.eventGroup
      ?: failGrpc(Status.NOT_FOUND) { "EventGroup not found" }
  }

  override suspend fun deleteEventGroup(request: DeleteEventGroupRequest): EventGroup {
    grpcRequire(request.externalDataProviderId > 0L) { "ExternalDataProviderId unspecified" }
    grpcRequire(request.externalEventGroupId > 0L) { "ExternalEventGroupId unspecified" }

    val eventGroup = eventGroup {
      externalDataProviderId = request.externalDataProviderId
      externalEventGroupId = request.externalEventGroupId
    }

    try {
      return DeleteEventGroup(eventGroup).execute(client, idGenerator)
    } catch (e: EventGroupNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "EventGroup not found." }
    } catch (e: EventGroupStateIllegalException) {
      when (e.state) {
        EventGroup.State.DELETED ->
          e.throwStatusRuntimeException(Status.NOT_FOUND) { "EventGroup state is DELETED." }
        EventGroup.State.ACTIVE,
        EventGroup.State.STATE_UNSPECIFIED,
        EventGroup.State.UNRECOGNIZED ->
          e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
      }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override fun streamEventGroups(request: StreamEventGroupsRequest): Flow<EventGroup> {
    return StreamEventGroups(request.filter, request.limit).execute(client.singleUse()).map {
      it.eventGroup
    }
  }
}
