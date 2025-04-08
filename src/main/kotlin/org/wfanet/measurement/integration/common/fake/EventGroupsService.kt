/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.fake

import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DeleteEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse

class EventGroupsService(private val eventGroups: List<EventGroup>) :
  EventGroupsCoroutineImplBase() {
  private val updatedEventGroups: MutableList<EventGroup> = eventGroups.toMutableList()
  private var index = 0

  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    return updatedEventGroups.filter { request.name == it.name }.single()
  }

  override suspend fun createEventGroup(request: CreateEventGroupRequest): EventGroup {
    updatedEventGroups.add(request.eventGroup)

    return request.eventGroup
  }

  override suspend fun updateEventGroup(request: UpdateEventGroupRequest): EventGroup {
    updatedEventGroups.removeIf { request.eventGroup.name == it.name }
    updatedEventGroups.add(request.eventGroup)
    return request.eventGroup
  }

  override suspend fun deleteEventGroup(request: DeleteEventGroupRequest): EventGroup {
    val deletedEventGroup = updatedEventGroups.filter { request.name == it.name }.single()
    updatedEventGroups.removeIf { request.name == it.name }
    return deletedEventGroup
  }

  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    return listEventGroupsResponse {
      val numToReturn = minOf(request.pageSize, updatedEventGroups.size - index)
      if (numToReturn > 0) {
        this.eventGroups += updatedEventGroups.subList(index, index + numToReturn)
        this.nextPageToken = "some-fake-token"
        index += 1
      }
    }
  }
}
