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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.util.Timestamps
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageInvalidArgsException

class CreateModelOutage(private val modelOutage: ModelOutage) :
  SpannerWriter<ModelOutage, ModelOutage>() {

  override suspend fun TransactionScope.runTransaction(): ModelOutage {

    val modelLineData: Struct =
      readModelLineData(
        ExternalId(modelOutage.externalModelProviderId),
        ExternalId(modelOutage.externalModelSuiteId),
        ExternalId(modelOutage.externalModelLineId)
      )
        ?: throw ModelLineNotFoundException(
          ExternalId(modelOutage.externalModelProviderId),
          ExternalId(modelOutage.externalModelSuiteId),
          ExternalId(modelOutage.externalModelLineId)
        )

    if (Timestamps.compare(modelOutage.modelOutageStartTime, modelOutage.modelOutageEndTime) >= 0) {
      throw ModelOutageInvalidArgsException(
        ExternalId(modelOutage.externalModelProviderId),
        ExternalId(modelOutage.externalModelSuiteId),
        ExternalId(modelOutage.externalModelLineId),
      ) {
        "ModelOutageStartTime cannot precede ModelOutageEndTime."
      }
    }

    val internalModelOutageId = idGenerator.generateInternalId()
    val externalModelOutageId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelOutages") {
      set("ModelProviderId" to InternalId(modelLineData.getLong("ModelProviderId")))
      set("ModelSuiteId" to InternalId(modelLineData.getLong("ModelSuiteId")))
      set("ModelLineId" to modelLineData.getLong("ModelLineId"))
      set("ModelOutageId" to internalModelOutageId)
      set("ExternalModelOutageId" to externalModelOutageId)
      set("OutageStartTime" to modelOutage.modelOutageStartTime.toGcloudTimestamp())
      set("OutageEndTime" to modelOutage.modelOutageEndTime.toGcloudTimestamp())
      set("State" to ModelOutage.State.ACTIVE)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelOutage.copy {
      this.externalModelOutageId = externalModelOutageId.value
      this.state = ModelOutage.State.ACTIVE
    }
  }

  private suspend fun TransactionScope.readModelLineData(
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId
  ): Struct? {
    val sql =
      """
    SELECT
    ModelLines.ModelProviderId,
    ModelLines.ModelSuiteId,
    ModelLines.ModelLineId
    FROM ModelSuites JOIN ModelProviders USING(ModelProviderId)
    JOIN ModelLines USING(ModelSuiteId)
    WHERE ExternalModelProviderId = @externalModelProviderId
    AND ExternalModelSuiteId = @externalModelSuiteId
    AND ExternalModelLineId = @externalModelLineId
    """
        .trimIndent()

    val statement: Statement =
      statement(sql) {
        bind("externalModelProviderId" to externalModelProviderId.value)
        bind("externalModelSuiteId" to externalModelSuiteId.value)
        bind("externalModelLineId" to externalModelLineId.value)
      }

    return transactionContext.executeQuery(statement).singleOrNull()
  }

  override fun ResultScope<ModelOutage>.buildResult(): ModelOutage {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
