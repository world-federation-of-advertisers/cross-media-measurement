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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.CreateIncidentRequest
import org.wfanet.measurement.internal.kingdom.Incident
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class CreateIncident(private val request: CreateIncidentRequest): SpannerWriter<Incident, Incident>() {
  override suspend fun TransactionScope.runTransaction(): Incident {
    val incidentId: InternalId = idGenerator.generateInternalId()
    val externalIncidentId: ExternalId = idGenerator.generateExternalId()

    val dataProviderId: Long? =
      if (request.incident.externalDataProviderId != 0L) {
        DataProviderReader().readByExternalDataProviderId(transactionContext, ExternalId(request.incident.externalDataProviderId))
          ?.dataProviderId
          ?: throw DataProviderNotFoundException(ExternalId(request.incident.externalDataProviderId))
      } else {
        null
      }

    transactionContext.bufferInsertMutation("Incidents") {
      set("IncidentId" to incidentId)
      set("ExternalIncidentId" to externalIncidentId)
      set("DataProviderId" to dataProviderId)
      set("Description" to request.incident.description)
      set("Solution" to request.incident.solution)
      set("CanRerun" to request.incident.canRerun)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return request.incident.copy {
      this.externalIncidentId = externalIncidentId.value
    }
  }

  override fun ResultScope<Incident>.buildResult(): Incident {
    return transactionResult!!.copy {
      createTime = commitTimestamp.toProto()
    }
  }
}
