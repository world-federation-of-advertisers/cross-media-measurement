package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class CreatePopulation(private val population: Population) :
  SpannerWriter<Population, Population>() {
  override suspend fun TransactionScope.runTransaction(): Population {
    val externalDataProviderId = ExternalId(population.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    val dataProviderId = dataProviderResult.dataProviderId

    val internalPopulationId = idGenerator.generateInternalId()
    val externalPopulationId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("Populations") {
      set("DataProviderId" to dataProviderId)
      set("PopulationId" to internalPopulationId)
      set("ExternalPopulationId" to externalPopulationId)
      set("Description" to population.description)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("ModelBlobUri" to population.populationBlob.modelBlobUri)
      set("EventTemplateType" to population.eventTemplate.fullyQualifiedType)
    }

    return population.copy { this.externalPopulationId = externalPopulationId.value }
  }

  override fun ResultScope<Population>.buildResult(): Population {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
