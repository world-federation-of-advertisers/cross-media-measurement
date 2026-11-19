// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.SetModelLineEventTemplateVersionRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

/** Initializes historical model-line metadata without allowing a version change. */
class SetModelLineEventTemplateVersion(
  private val request: SetModelLineEventTemplateVersionRequest
) : SpannerWriter<SetModelLineEventTemplateVersion.Result, ModelLine>() {
  data class Result(val modelLine: ModelLine, val initialized: Boolean)

  override suspend fun TransactionScope.runTransaction(): Result {
    val providerId = ExternalId(request.externalModelProviderId)
    val suiteId = ExternalId(request.externalModelSuiteId)
    val lineId = ExternalId(request.externalModelLineId)
    val result =
      ModelLineReader().readByExternalModelLineId(txn, providerId, suiteId, lineId)
        ?: throw ModelLineNotFoundException(providerId, suiteId, lineId)
    val modelLine = result.modelLine
    if (modelLine.hasEventTemplateVersion()) {
      if (modelLine.eventTemplateVersion != request.eventTemplateVersion) {
        throw ModelLineInvalidArgsException(
          providerId,
          suiteId,
          lineId,
          "event_template_version cannot be changed once initialized",
        )
      }
      return Result(modelLine, false)
    }
    txn.bufferUpdateMutation("ModelLines") {
      set("ModelProviderId").to(result.modelProviderId)
      set("ModelSuiteId").to(result.modelSuiteId)
      set("ModelLineId").to(result.modelLineId)
      set("EventTemplateVersion").to(request.eventTemplateVersion.toLong())
      set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
    }
    return Result(modelLine.copy { eventTemplateVersion = request.eventTemplateVersion }, true)
  }

  override fun ResultScope<Result>.buildResult(): ModelLine {
    val result = checkNotNull(transactionResult)
    return if (result.initialized) result.modelLine.copy { updateTime = commitTimestamp.toProto() }
    else result.modelLine
  }
}
