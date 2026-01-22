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
import java.time.Clock
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.SetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class SetActiveEndTime(private val request: SetActiveEndTimeRequest, private val clock: Clock) :
  SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {
    val modelLineResult =
      ModelLineReader()
        .readByExternalModelLineId(
          transactionContext,
          ExternalId(request.externalModelProviderId),
          ExternalId(request.externalModelSuiteId),
          ExternalId(request.externalModelLineId),
        )
        ?: throw ModelLineNotFoundException(
          ExternalId(request.externalModelProviderId),
          ExternalId(request.externalModelSuiteId),
          ExternalId(request.externalModelLineId),
        )

    val now = clock.instant().toProtoTime()
    val activeStartTime = modelLineResult.modelLine.activeStartTime

    if (Timestamps.compare(now, request.activeEndTime) >= 0) {
      throw ModelLineInvalidArgsException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        "ActiveEndTime must be in the future.",
      )
    }

    if (Timestamps.compare(activeStartTime, request.activeEndTime) >= 0) {
      throw ModelLineInvalidArgsException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        "ActiveEndTime must be later than ActiveStartTime.",
      )
    }

    transactionContext.bufferUpdateMutation("ModelLines") {
      set("ModelLineId" to modelLineResult.modelLineId.value)
      set("ModelSuiteId" to modelLineResult.modelSuiteId.value)
      set("ModelProviderId" to modelLineResult.modelProviderId.value)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("ActiveEndTime" to request.activeEndTime.toGcloudTimestamp())
    }

    return modelLineResult.modelLine.copy { activeEndTime = request.activeEndTime }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
