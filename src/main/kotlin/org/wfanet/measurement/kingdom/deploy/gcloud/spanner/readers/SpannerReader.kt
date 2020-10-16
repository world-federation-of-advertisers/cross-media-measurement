// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Statement
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause

/**
 * Abstraction for reading rows from Spanner and translating into more expressive objects.
 */
abstract class SpannerReader<T : Any> : BaseSpannerReader<T>() {
  protected abstract val baseSql: String
  protected abstract val externalIdColumn: String

  override val builder: Statement.Builder by lazy {
    Statement.newBuilder(baseSql)
  }

  fun withBuilder(block: Statement.Builder.() -> Unit): SpannerReader<T> {
    builder.block()
    return this
  }

  suspend fun readExternalIdOrNull(
    readContext: AsyncDatabaseClient.ReadContext,
    externalId: ExternalId
  ): T? {
    return this
      .withBuilder {
        appendClause("WHERE $externalIdColumn = @external_id")
        bind("external_id").to(externalId.value)

        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalId: ExternalId
  ): T {
    return requireNotNull(readExternalIdOrNull(readContext, externalId)) {
      "Not found: $externalId"
    }
  }
}
