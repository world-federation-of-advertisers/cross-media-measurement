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
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader

/**
 * [SpannerWriter] for creating a [ModelLine].
 *
 * May throw one of the following on [execute]:
 * * [ModelSuiteNotFoundException] if the parent model suite is not found
 * * [ModelLineNotFoundException] if the holdback [ModelLine] is not found
 * * [ModelLineTypeIllegalException] if the holdback [ModelLine] has an invalid type
 */
class CreateModelLine(private val modelLine: ModelLine) : SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {
    val externalModelProviderId = ExternalId(modelLine.externalModelProviderId)
    val externalModelSuiteId = ExternalId(modelLine.externalModelSuiteId)

    val modelSuiteKey =
      ModelSuiteReader.readModelSuiteKey(
        transactionContext,
        externalModelProviderId,
        externalModelSuiteId,
      )
        ?: throw ModelSuiteNotFoundException(
          ExternalId(modelLine.externalModelProviderId),
          ExternalId(modelLine.externalModelSuiteId),
        )

    val internalModelLineId = idGenerator.generateInternalId()
    val externalModelLineId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelLines") {
      set("ModelProviderId").to(modelSuiteKey.modelProviderId)
      set("ModelSuiteId").to(modelSuiteKey.modelSuiteId)
      set("ModelLineId" to internalModelLineId)
      set("ExternalModelLineId" to externalModelLineId)
      if (modelLine.displayName.isNotBlank()) {
        set("DisplayName" to modelLine.displayName)
      }
      if (modelLine.description.isNotBlank()) {
        set("Description" to modelLine.description)
      }
      set("ActiveStartTime" to modelLine.activeStartTime.toGcloudTimestamp())
      if (modelLine.hasActiveEndTime()) {
        set("ActiveEndTime" to modelLine.activeEndTime.toGcloudTimestamp())
      }
      set("Type").toInt64(modelLine.type)
      if (modelLine.externalHoldbackModelLineId != 0L) {
        val holdbackModelLineResult =
          ModelLineReader()
            .readByExternalModelLineId(
              transactionContext,
              externalModelProviderId,
              externalModelSuiteId,
              ExternalId(modelLine.externalHoldbackModelLineId),
            )
            ?: throw ModelLineNotFoundException(
              externalModelProviderId,
              externalModelSuiteId,
              ExternalId(modelLine.externalHoldbackModelLineId),
            )

        if (holdbackModelLineResult.modelLine.type != ModelLine.Type.HOLDBACK) {
          throw ModelLineTypeIllegalException(
            ExternalId(holdbackModelLineResult.modelLine.externalModelProviderId),
            ExternalId(holdbackModelLineResult.modelLine.externalModelSuiteId),
            ExternalId(holdbackModelLineResult.modelLine.externalModelLineId),
            holdbackModelLineResult.modelLine.type,
          )
        }
        set("HoldbackModelLineId" to holdbackModelLineResult.modelLineId)
      }
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelLine.copy { this.externalModelLineId = externalModelLineId.value }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }
}
