package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest

class SpannerEventGroupsService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : EventGroupsCoroutineImplBase() {
  override suspend fun createEventGroup(request: EventGroup): EventGroup {
    TODO("not implemented yet")
  }
  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    TODO("not implemented yet")
  }
}
