// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.insertOrUpdateMutation
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsContinuationToken

class SetContinuationToken(val continuationToken: String) {
  suspend fun execute(databaseClient: AsyncDatabaseClient) {
    databaseClient.readWriteTransaction().execute { ctx ->
      val newContinuationToken = continuationToken.decode()
      val oldContinuationToken =
        ctx
          .readRow("HeraldContinuationTokens", Key.of(true), listOf("ContinuationToken"))
          ?.getString("ContinuationToken")
          ?.decode()
          ?: StreamActiveComputationsContinuationToken.getDefaultInstance()

      if (
        Timestamps.compare(
          newContinuationToken.updateTimeSince,
          oldContinuationToken.updateTimeSince
        ) >= 0
      ) {
        val mutation =
          insertOrUpdateMutation("HeraldContinuationTokens") {
            set("Presence").to(true)
            set("ContinuationToken").to(continuationToken)
            set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
          }
        ctx.buffer(mutation)
      } else {
        throw IllegalArgumentException("ContinuationToken to set cannot have older timestamp.")
      }
    }
  }

  private fun String.decode(): StreamActiveComputationsContinuationToken =
    StreamActiveComputationsContinuationToken.parseFrom(this.base64UrlDecode())
}
