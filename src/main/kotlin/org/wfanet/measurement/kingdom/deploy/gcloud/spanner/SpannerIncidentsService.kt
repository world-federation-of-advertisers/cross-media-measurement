/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.CreateIncidentRequest
import org.wfanet.measurement.internal.kingdom.GetIncidentRequest
import org.wfanet.measurement.internal.kingdom.Incident
import org.wfanet.measurement.internal.kingdom.IncidentsGrpcKt.IncidentsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ListIncidentsRequest
import org.wfanet.measurement.internal.kingdom.ListIncidentsResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateIncident

class SpannerIncidentsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
) : IncidentsCoroutineImplBase() {
  override suspend fun createIncident(request: CreateIncidentRequest): Incident {
    try {
      return CreateIncident(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION, "DataProvider not found.")
    }
  }

  override suspend fun getIncident(request: GetIncidentRequest): Incident {
    TODO("Not yet implemented")
  }

  override suspend fun listIncidents(request: ListIncidentsRequest): ListIncidentsResponse {
    TODO("Not yet implemented")
  }
}
