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

import com.google.cloud.spanner.SpannerException
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.wrappedException
import org.wfanet.measurement.internal.kingdom.EventGroup
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.GetEventGroupRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.EventGroupReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateEventGroup

class SpannerEventGroupsService(
  val clock: Clock,
  val idGenerator: IdGenerator,
  val client: AsyncDatabaseClient
) : EventGroupsCoroutineImplBase() {
  override suspend fun createEventGroup(request: EventGroup): EventGroup {
    try {
      return CreateEventGroup(request).execute(client, idGenerator, clock)
    } catch (e: SpannerException) {
      throw e.wrappedException ?: e
    } catch (e: IllegalArgumentException) {
      println(e)
      throw StatusRuntimeException(
        Status.fromCode(Status.Code.NOT_FOUND),
        Status.trailersFromThrowable(e)
      )
    }
  }
  override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup {
    return EventGroupReader()
      .readExternalIdOrNull(client.singleUse(), ExternalId(request.externalEventGroupId))
      ?.eventGroup
      ?: failGrpc(Status.NOT_FOUND) {
        "No EventGroup with externalId ${request.externalEventGroupId}"
      }
  }
}
