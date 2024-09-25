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
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelOutageReader

/**
 * Soft Delete [ModelOutage] in the database.
 *
 * Throws one of the following [KingdomInternalException] types on [execute]:
 * * [ModelOutageNotFoundException] ModelOutage not found
 * * [ModelOutageStateIllegalException] ModelOutage state is DELETED
 */
class DeleteModelOutage(private val modelOutage: ModelOutage) :
  SpannerWriter<ModelOutage, ModelOutage>() {

  override suspend fun TransactionScope.runTransaction(): ModelOutage {
    val internalModelOutageResult =
      ModelOutageReader()
        .readByExternalIds(
          transactionContext,
          ExternalId(modelOutage.externalModelProviderId),
          ExternalId(modelOutage.externalModelSuiteId),
          ExternalId(modelOutage.externalModelLineId),
          ExternalId(modelOutage.externalModelOutageId),
        )
        ?: throw ModelOutageNotFoundException(
          ExternalId(modelOutage.externalModelProviderId),
          ExternalId(modelOutage.externalModelSuiteId),
          ExternalId(modelOutage.externalModelLineId),
          ExternalId(modelOutage.externalModelOutageId),
        )

    if (internalModelOutageResult.modelOutage.state == ModelOutage.State.DELETED) {
      throw ModelOutageStateIllegalException(
        ExternalId(modelOutage.externalModelProviderId),
        ExternalId(modelOutage.externalModelSuiteId),
        ExternalId(modelOutage.externalModelLineId),
        ExternalId(modelOutage.externalModelOutageId),
        internalModelOutageResult.modelOutage.state,
      )
    }

    transactionContext.bufferUpdateMutation("ModelOutages") {
      set("ModelProviderId" to internalModelOutageResult.modelProviderId.value)
      set("ModelSuiteId" to internalModelOutageResult.modelSuiteId.value)
      set("ModelLineId" to internalModelOutageResult.modelLineId.value)
      set("ModelOutageId" to internalModelOutageResult.modelOutageId.value)
      set("State").toInt64(ModelOutage.State.DELETED)
      set("DeleteTime" to Value.COMMIT_TIMESTAMP)
    }

    return internalModelOutageResult.modelOutage.copy { this.state = ModelOutage.State.DELETED }
  }

  override fun ResultScope<ModelOutage>.buildResult(): ModelOutage {
    val modelOutage = checkNotNull(transactionResult)
    return modelOutage.copy { deleteTime = commitTimestamp.toProto() }
  }
}
