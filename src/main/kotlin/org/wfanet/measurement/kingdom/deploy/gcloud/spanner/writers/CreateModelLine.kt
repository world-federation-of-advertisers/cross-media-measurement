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
import java.time.Clock
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class CreateModelLine(private val modelLine: ModelLine, private val clock: Clock) :
  SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {

    val now = clock.instant().toProtoTime()
    if (Timestamps.compare(now, modelLine.activeStartTime) >= 0) {
      throw ModelLineInvalidArgsException(
        ExternalId(modelLine.externalModelProviderId),
        ExternalId(modelLine.externalModelSuiteId)
      ) {
        "ActiveStartTime must be in the future."
      }
    }

    if (
      modelLine.hasActiveEndTime() &&
        Timestamps.compare(modelLine.activeStartTime, modelLine.activeEndTime) >= 0
    ) {
      throw ModelLineInvalidArgsException(
        ExternalId(modelLine.externalModelProviderId),
        ExternalId(modelLine.externalModelSuiteId)
      ) {
        "ActiveEndTime cannot precede ActiveStartTime."
      }
    }

    val modelSuiteData: Struct =
      readModelSuiteData(
        ExternalId(modelLine.externalModelProviderId),
        ExternalId(modelLine.externalModelSuiteId)
      )
        ?: throw ModelSuiteNotFoundException(
          ExternalId(modelLine.externalModelProviderId),
          ExternalId(modelLine.externalModelSuiteId)
        )

    val internalModelLineId = idGenerator.generateInternalId()
    val externalModelLineId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelLines") {
      set("ModelProviderId" to InternalId(modelSuiteData.getLong("ModelProviderId")))
      set("ModelSuiteId" to InternalId(modelSuiteData.getLong("ModelSuiteId")))
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
      set("Type" to modelLine.type)
      if (modelLine.externalHoldbackModelLineId != 0L) {
        val holdbackModelLineResult =
          ModelLineReader()
            .readByExternalModelLineId(
              transactionContext,
              ExternalId(modelLine.externalModelProviderId),
              ExternalId(modelLine.externalModelSuiteId),
              ExternalId(modelLine.externalHoldbackModelLineId)
            )
            ?: throw ModelLineNotFoundException(
              ExternalId(modelLine.externalModelProviderId),
              ExternalId(modelLine.externalModelSuiteId),
              ExternalId(modelLine.externalHoldbackModelLineId)
            ) {
              "HoldbackModelLine not found."
            }

        if (modelLine.type != ModelLine.Type.PROD) {
          throw ModelLineTypeIllegalException(
            ExternalId(modelLine.externalModelProviderId),
            ExternalId(modelLine.externalModelSuiteId),
            externalModelLineId,
            modelLine.type
          ) {
            "Only ModelLine with type == PROD can have a Holdback ModelLine."
          }
        }
        if (holdbackModelLineResult.modelLine.type != ModelLine.Type.HOLDBACK) {
          throw ModelLineTypeIllegalException(
            ExternalId(holdbackModelLineResult.modelLine.externalModelProviderId),
            ExternalId(holdbackModelLineResult.modelLine.externalModelSuiteId),
            ExternalId(holdbackModelLineResult.modelLine.externalModelLineId),
            holdbackModelLineResult.modelLine.type
          ) {
            "Only ModelLine with type == HOLDBACK can be set as Holdback ModelLine."
          }
        }

        set("HoldbackModelLine" to holdbackModelLineResult.modelLineId)
      }
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelLine.copy { this.externalModelLineId = externalModelLineId.value }
  }

  private suspend fun TransactionScope.readModelSuiteData(
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId
  ): Struct? {
    val sql =
      """
    SELECT
    ModelSuites.ModelSuiteId,
    ModelSuites.ModelProviderId
    FROM ModelSuites JOIN ModelProviders USING(ModelProviderId)
    WHERE ExternalModelSuiteId = @externalModelSuiteId AND ExternalModelProviderId = @externalModelProviderId
    """
        .trimIndent()

    val statement: Statement =
      statement(sql) {
        bind("externalModelSuiteId" to externalModelSuiteId.value)
        bind("externalModelProviderId" to externalModelProviderId.value)
      }

    return transactionContext.executeQuery(statement).singleOrNull()
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }
}
