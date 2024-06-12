// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader

/** Abstraction around a common pattern for Spanner read-only queries. */
interface SpannerQuery<T> {
  /** Executes the query. */
  fun execute(readContext: AsyncDatabaseClient.ReadContext): Flow<T>
}

/** [SpannerQuery] which wraps a [BaseSpannerReader]. */
abstract class SimpleSpannerQuery<T : Any> : SpannerQuery<T> {
  /** The [BaseSpannerReader] to execute. */
  protected abstract val reader: BaseSpannerReader<T>

  override fun execute(readContext: AsyncDatabaseClient.ReadContext): Flow<T> {
    return reader.execute(readContext)
  }
}
