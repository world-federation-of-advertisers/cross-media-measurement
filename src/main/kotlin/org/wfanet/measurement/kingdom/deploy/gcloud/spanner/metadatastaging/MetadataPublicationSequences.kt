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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.metadatastaging

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

object MetadataPublicationSequences {
  /**
   * Allocates one version per scope per transaction, shared by all metadata writes in that scope.
   */
  suspend fun next(
    transaction: AsyncDatabaseClient.TransactionContext,
    dataProviderId: Long,
    measurementConsumerId: Long,
  ): Long {
    val scope =
      transaction.readRow(
        "EventGroupMetadataPublicationScopes",
        Key.of(dataProviderId, measurementConsumerId),
        listOf("PublicationSequence"),
      )
    val next = Math.addExact(scope?.getLong("PublicationSequence") ?: 0L, 1L)
    transaction.buffer(
      Mutation.newInsertOrUpdateBuilder("EventGroupMetadataPublicationScopes")
        .apply {
          set("DataProviderId").to(dataProviderId)
          set("MeasurementConsumerId").to(measurementConsumerId)
          set("PublicationSequence").to(next)
        }
        .build()
    )
    return next
  }
}
