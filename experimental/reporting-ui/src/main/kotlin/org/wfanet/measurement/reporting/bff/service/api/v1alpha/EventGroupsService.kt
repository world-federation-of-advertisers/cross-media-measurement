// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.bff.service.api.v1alpha

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.reporting.bff.v1alpha.ListEventGroupsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListEventGroupsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.bff.v1alpha.eventGroup
import org.wfanet.measurement.reporting.bff.v1alpha.listEventGroupsResponse
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt as HaloEventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest

class EventGroupsService(private val haloEventGroupsStub: HaloEventGroupsGrpcKt.EventGroupsCoroutineStub) :
  EventGroupsGrpcKt.EventGroupsCoroutineImplBase() {
  override suspend fun listEventGroups(request: ListEventGroupsRequest): ListEventGroupsResponse {
    val haloRequest = listEventGroupsRequest {
      parent = request.parent
      pageSize = if (request.pageSize > 0) request.pageSize else 1000
      filter = request.filter
    }

    val resp = runBlocking(Dispatchers.IO) { haloEventGroupsStub.listEventGroups(haloRequest) }

    val results = listEventGroupsResponse {
      nextPageToken = resp.nextPageToken

      resp.eventGroupsList.forEach {
        val e = eventGroup {
            name = it.name
            cmmsEventGroup = it.cmmsEventGroup
            cmmsDataProvider = it.cmmsDataProvider
            eventGroupReferenceId = it.eventGroupReferenceId
        }
        eventGroups += e
      }
    }
    return results
  }
}
