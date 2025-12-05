/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.SetModelLineTypeRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class SetModelLineType(private val request: SetModelLineTypeRequest) :
  SpannerWriter<ModelLine, ModelLine>() {
  init {
    require(request.type != ModelLine.Type.HOLDBACK)
  }

  override suspend fun TransactionScope.runTransaction(): ModelLine {
    val externalModelProviderId = ExternalId(request.externalModelProviderId)
    val externalModelSuiteId = ExternalId(request.externalModelSuiteId)
    val externalModelLineId = ExternalId(request.externalModelLineId)

    val result: ModelLineReader.Result =
      ModelLineReader()
        .readByExternalModelLineId(
          txn,
          externalModelProviderId,
          externalModelSuiteId,
          externalModelLineId,
        )
        ?: throw ModelLineNotFoundException(
          externalModelProviderId,
          externalModelSuiteId,
          externalModelLineId,
        )
    if (result.modelLine.type == ModelLine.Type.HOLDBACK) {
      throw ModelLineTypeIllegalException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        result.modelLine.type,
      )
    }
    if (request.type == result.modelLine.type) {
      throw ModelLineTypeIllegalException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        result.modelLine.type,
      )
    }
    if (result.modelLine.externalHoldbackModelLineId != 0L) {
      throw ModelLineInvalidArgsException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        "Cannot change the type of a ModelLine with a holdback",
      )
    }

    txn.bufferUpdateMutation("ModelLines") {
      set("ModelProviderId").to(result.modelProviderId)
      set("ModelSuiteId").to(result.modelSuiteId)
      set("ModelLineId").to(result.modelLineId)
      set("Type").to(request.type)
      set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    }

    return result.modelLine.copy { type = request.type }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
