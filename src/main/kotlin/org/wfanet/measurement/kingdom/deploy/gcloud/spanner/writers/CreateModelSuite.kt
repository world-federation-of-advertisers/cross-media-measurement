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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderNotFoundException

class CreateModelSuite(private val modelSuite: ModelSuite) :
  SpannerWriter<ModelSuite, ModelSuite>() {

  override suspend fun TransactionScope.runTransaction(): ModelSuite {

    val modelProviderId: InternalId =
      readModelProviderId(ExternalId(modelSuite.externalModelProviderId))

    val internalModelSuiteId = idGenerator.generateInternalId()
    val externalModelSuiteId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelSuites") {
      set("ModelProviderId" to modelProviderId)
      set("ModelSuiteId" to internalModelSuiteId)
      set("ExternalModelSuiteId" to externalModelSuiteId)
      set("DisplayName" to modelSuite.displayName)
      set("Description" to modelSuite.description)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelSuite.copy { this.externalModelSuiteId = externalModelSuiteId.value }
  }

  private suspend fun TransactionScope.readModelProviderId(
    externalModelProviderId: ExternalId
  ): InternalId {
    val column = "ModelProviderId"
    return transactionContext
      .readRowUsingIndex(
        "ModelProviders",
        "ModelProvidersByExternalId",
        Key.of(externalModelProviderId.value),
        column,
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw ModelProviderNotFoundException(externalModelProviderId) {
        "ModelProvider with external ID $externalModelProviderId not found"
      }
  }

  override fun ResultScope<ModelSuite>.buildResult(): ModelSuite {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
