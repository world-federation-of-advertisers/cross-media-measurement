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
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class SetActiveStartTime(
  private val externalModelProviderId: ExternalId,
  private val externalModelSuiteId: ExternalId,
  private val externalModelLineId: ExternalId,
  private val activeStartTime: Timestamp,
) : SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {
    val modelLineResult =
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

    val activeEndTime = modelLineResult.modelLine.activeEndTime

    if (
      modelLineResult.modelLine.hasActiveEndTime() &&
        Timestamps.compare(activeStartTime, activeEndTime) >= 0
    ) {
      throw ModelLineInvalidArgsException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        "ActiveStartTime must be before ActiveEndTime.",
      )
    }

    transactionContext.bufferUpdateMutation("ModelLines") {
      set("ModelLineId" to modelLineResult.modelLineId.value)
      set("ModelSuiteId" to modelLineResult.modelSuiteId.value)
      set("ModelProviderId" to modelLineResult.modelProviderId.value)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ActiveStartTime" to activeStartTime.toGcloudTimestamp())
    }

    return modelLineResult.modelLine.copy { activeStartTime = this@SetActiveStartTime.activeStartTime }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
