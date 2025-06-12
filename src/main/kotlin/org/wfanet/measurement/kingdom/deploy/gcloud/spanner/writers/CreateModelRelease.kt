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
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PopulationNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.PopulationReader

class CreateModelRelease(private val modelRelease: ModelRelease) :
  SpannerWriter<ModelRelease, ModelRelease>() {

  override suspend fun TransactionScope.runTransaction(): ModelRelease {

    val externalModelProviderId = ExternalId(modelRelease.externalModelProviderId)
    val externalModelSuiteId = ExternalId(modelRelease.externalModelSuiteId)
    val modelSuiteResult: ModelSuiteReader.Result =
      ModelSuiteReader()
        .readByExternalModelSuiteId(
          transactionContext,
          externalModelProviderId,
          externalModelSuiteId,
        ) ?: throw ModelSuiteNotFoundException(externalModelProviderId, externalModelSuiteId)

    val externalDataProviderId = ExternalId(modelRelease.externalDataProviderId)
    val externalPopulationId = ExternalId(modelRelease.externalPopulationId)
    val populationResult: PopulationReader.Result =
      PopulationReader()
        .readByExternalPopulationId(
          transactionContext,
          externalDataProviderId,
          externalPopulationId,
        ) ?: throw PopulationNotFoundException(externalDataProviderId, externalPopulationId)

    val internalModelReleaseId = idGenerator.generateInternalId()
    val externalModelReleaseId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelReleases") {
      set("ModelProviderId").to(modelSuiteResult.modelProviderId)
      set("ModelSuiteId").to(modelSuiteResult.modelSuiteId)
      set("ModelReleaseId" to internalModelReleaseId)
      set("ExternalModelReleaseId" to externalModelReleaseId)
      set("PopulationDataProviderId").to(populationResult.dataProviderId)
      set("PopulationId").to(populationResult.populationId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelRelease.copy { this.externalModelReleaseId = externalModelReleaseId.value }
  }

  override fun ResultScope<ModelRelease>.buildResult(): ModelRelease {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
