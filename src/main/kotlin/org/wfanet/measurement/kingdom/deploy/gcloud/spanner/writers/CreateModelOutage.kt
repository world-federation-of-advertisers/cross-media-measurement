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
import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class CreateModelOutage(private val modelOutage: ModelOutage) :
  SpannerWriter<ModelOutage, ModelOutage>() {

  override suspend fun TransactionScope.runTransaction(): ModelOutage {
    val externalModelProviderId = ExternalId(modelOutage.externalModelProviderId)
    val externalModelSuiteId = ExternalId(modelOutage.externalModelSuiteId)
    val externalModelLineId = ExternalId(modelOutage.externalModelLineId)
    val modelLineResult: ModelLineReader.Result =
      ModelLineReader()
        .readByExternalModelLineId(
          transactionContext,
          externalModelProviderId,
          externalModelSuiteId,
          externalModelLineId,
        )
        ?: throw ModelLineNotFoundException(
          externalModelProviderId,
          externalModelSuiteId,
          externalModelLineId,
        )

    if (Timestamps.compare(modelOutage.modelOutageStartTime, modelOutage.modelOutageEndTime) >= 0) {
      throw ModelOutageInvalidArgsException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
      ) {
        "ModelOutageStartTime cannot precede ModelOutageEndTime."
      }
    }

    val modelLineType: ModelLine.Type = modelLineResult.modelLine.type
    if (modelLineType != ModelLine.Type.PROD) {
      throw ModelOutageInvalidArgsException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
      ) {
        "ModelOutage can be created only for model lines having type equal to 'PROD'."
      }
    }

    if (modelLineResult.modelLine.externalHoldbackModelLineId == 0L) {
      throw ModelOutageInvalidArgsException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
      ) {
        "ModelOutage can be created only for model lines having a HoldbackModelLine."
      }
    }

    val internalModelOutageId = idGenerator.generateInternalId()
    val externalModelOutageId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelOutages") {
      set("ModelProviderId").to(modelLineResult.modelProviderId)
      set("ModelSuiteId").to(modelLineResult.modelSuiteId)
      set("ModelLineId").to(modelLineResult.modelLineId)
      set("ModelOutageId" to internalModelOutageId)
      set("ExternalModelOutageId" to externalModelOutageId)
      set("OutageStartTime" to modelOutage.modelOutageStartTime.toGcloudTimestamp())
      set("OutageEndTime" to modelOutage.modelOutageEndTime.toGcloudTimestamp())
      set("State").toInt64(ModelOutage.State.ACTIVE)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelOutage.copy {
      this.externalModelOutageId = externalModelOutageId.value
      this.state = ModelOutage.State.ACTIVE
    }
  }

  override fun ResultScope<ModelOutage>.buildResult(): ModelOutage {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
