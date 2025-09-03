/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.PopulationReader

class CreatePopulation(private val population: Population, private val requestId: String?) :
  SpannerWriter<Population, Population>() {
  override suspend fun TransactionScope.runTransaction(): Population {
    val externalDataProviderId = ExternalId(population.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    val dataProviderId = InternalId(dataProviderResult.dataProviderId)

    if (requestId != null) {
      val result: PopulationReader.Result? =
        PopulationReader().readByCreateRequestId(transactionContext, dataProviderId, requestId)
      if (result != null) {
        return result.population
      }
    }

    val internalPopulationId = idGenerator.generateInternalId()
    val externalPopulationId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("Populations") {
      set("DataProviderId" to dataProviderId)
      set("PopulationId" to internalPopulationId)
      set("ExternalPopulationId" to externalPopulationId)
      set("Description" to population.description)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      if (requestId != null) {
        set("CreateRequestId").to(requestId)
      }
      if (population.hasDetails()) {
        set("PopulationDetails").to(population.details)
      }
      if (population.hasPopulationBlob()) {
        set("ModelBlobUri").to(population.populationBlob.blobUri)
      }
    }

    return population.copy {
      clearCreateTime()
      this.externalPopulationId = externalPopulationId.value
    }
  }

  override fun ResultScope<Population>.buildResult(): Population {
    val result: Population = checkNotNull(this.transactionResult)
    return if (result.hasCreateTime()) {
      result
    } else {
      result.copy { createTime = commitTimestamp.toProto() }
    }
  }
}
