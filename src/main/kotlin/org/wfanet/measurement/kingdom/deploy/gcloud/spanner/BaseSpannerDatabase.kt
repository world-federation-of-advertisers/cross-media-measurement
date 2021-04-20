// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.SpannerQuery
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SpannerWriter

/** Convenience base class for Spanner databases. */
open class BaseSpannerDatabase(
  protected val clock: Clock,
  protected val idGenerator: IdGenerator,
  protected val client: AsyncDatabaseClient
) {
  /** Executes a [SpannerWriter] with [client], [idGenerator], and [clock]. */
  protected suspend fun <R> SpannerWriter<*, R>.execute(): R {
    return execute(client, idGenerator, clock)
  }

  /** Executes a [SpannerQuery] in a single-use ReadContext from [client]. */
  protected fun <R> SpannerQuery<*, R>.execute(): Flow<R> {
    return execute(client.singleUse())
  }

  /** Executes a [SpannerQuery] to get a single result in a single-use ReadContext from [client]. */
  protected suspend fun <R> SpannerQuery<*, R>.executeSingle(): R {
    return executeSingle(client.singleUse())
  }
}
