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
import com.google.protobuf.util.Timestamps
import java.time.Clock
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.SetActiveEndTimeRequest

class SetActiveEndTime(private val request: SetActiveEndTimeRequest, private val clock: Clock) :
  SpannerWriter<ModelLine, ModelLine>() {

  override suspend fun TransactionScope.runTransaction(): ModelLine {
    val modelLineData = readModelLineData(ExternalId(request.externalModelLineId))

    require(modelLineData != null) { "ModelLine not found." }

    val now = clock.instant().toProtoTime()
    val activeStartTime = modelLineData.getTimestamp("ActiveStartTime").toProto()
    require(Timestamps.compare(activeStartTime, request.activeEndTime) < 0) {
      "ActiveEndTime must be later than ActiveStartTime"
    }
    require(Timestamps.compare(now, request.activeEndTime) < 0) {
      "ActiveEndTime must be in the future"
    }
  }

  private suspend fun TransactionScope.readModelLineData(externalModelLineId: ExternalId): Struct? {
    val sql =
      """
    SELECT
    ModelLines.ModelLineId,
    ModelLines.ActiveStartTime
    FROM ModelLines
    WHERE ExternalModelLineId = @externalModelLineId
    """
        .trimIndent()

    val statement: Statement =
      statement(sql) { bind("externalModelLineId" to externalModelLineId.value) }

    return transactionContext.executeQuery(statement).singleOrNull()
  }

  override fun ResultScope<ModelLine>.buildResult(): ModelLine {
    return checkNotNull(transactionResult)
  }
}
