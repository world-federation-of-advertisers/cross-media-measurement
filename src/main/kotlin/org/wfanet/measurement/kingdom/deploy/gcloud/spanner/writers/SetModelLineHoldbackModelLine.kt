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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class SetModelLineHoldbackModelLine(private val request: SetModelLineHoldbackModelLineRequest) :
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

    if (modelLineResult.modelLine.type != ModelLine.Type.PROD) {
      throw ModelLineTypeIllegalException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        modelLineResult.modelLine.type,
      )
    }

    val holdbackModelLineId: InternalId? =
      if (request.externalHoldbackModelLineId == 0L) {
        null
      } else {
        val holdbackModelLineResult =
          ModelLineReader()
            .readByExternalModelLineId(
              transactionContext,
              ExternalId(request.externalHoldbackModelProviderId),
              ExternalId(request.externalHoldbackModelSuiteId),
              ExternalId(request.externalHoldbackModelLineId),
            )
            ?: throw ModelLineNotFoundException(
              ExternalId(request.externalHoldbackModelProviderId),
              ExternalId(request.externalHoldbackModelSuiteId),
              ExternalId(request.externalHoldbackModelLineId),
            )

        if (holdbackModelLineResult.modelLine.type != ModelLine.Type.HOLDBACK) {
          throw ModelLineTypeIllegalException(
            ExternalId(holdbackModelLineResult.modelLine.externalModelProviderId),
            ExternalId(holdbackModelLineResult.modelLine.externalModelSuiteId),
            ExternalId(holdbackModelLineResult.modelLine.externalModelLineId),
            holdbackModelLineResult.modelLine.type,
          )
        }
        holdbackModelLineResult.modelLineId
      }

    transactionContext.bufferUpdateMutation("ModelLines") {
      set("ModelLineId" to modelLineResult.modelLineId.value)
      set("ModelSuiteId" to modelLineResult.modelSuiteId.value)
      set("ModelProviderId" to modelLineResult.modelProviderId.value)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("HoldbackModelLineId").to(holdbackModelLineId)
    }

    return modelLineResult.modelLine.copy {
      externalHoldbackModelLineId = request.externalHoldbackModelLineId
    }
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
