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
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.copy

class CreateModelRelease(private val modelRelease: ModelRelease) :
  SpannerWriter<ModelRelease, ModelRelease>() {

  override suspend fun TransactionScope.runTransaction(): ModelRelease {

    val modelSuiteData: Struct? = readModelSuiteData(ExternalId(modelRelease.externalModelSuiteId))

    require(modelSuiteData != null) { "ModelSuite not found." }

    val internalModelReleaseId = idGenerator.generateInternalId()
    val externalModelReleaseId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelReleases") {
      set("ModelProviderId" to modelSuiteData.getLong("ModelProviderId"))
      set("ModelSuiteId" to modelSuiteData.getLong("ModelSuiteId"))
      set("ModelReleaseId" to internalModelReleaseId)
      set("ExternalModelReleaseId" to externalModelReleaseId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelRelease.copy { this.externalModelReleaseId = externalModelReleaseId.value }
  }

  private suspend fun TransactionScope.readModelSuiteData(
    externalModelSuiteId: ExternalId
  ): Struct? {
    val sql =
      """
    SELECT
    ModelSuites.ModelSuiteId,
    ModelSuites.ModelProviderId
    FROM ModelSuites
    WHERE ExternalModelSuiteId = @externalModelSuiteId
    """
        .trimIndent()

    val statement: Statement =
      statement(sql) { bind("externalModelSuiteId" to externalModelSuiteId.value) }

    return transactionContext.executeQuery(statement).singleOrNull()
  }

  override fun ResultScope<ModelRelease>.buildResult(): ModelRelease {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
