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

package org.wfanet.measurement.db.kingdom.gcp.readers

import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.asFlow

/** Base abstraction for reading from Spanner. */
abstract class BaseSpannerReader<T> {
  /** Provides a Statement builder for modification -- this is used to execute the query. */
  abstract val builder: Statement.Builder

  /** Transforms the results of the query. */
  protected abstract suspend fun translate(struct: Struct): T

  /** Executes the query. */
  fun execute(readContext: ReadContext): Flow<T> =
    readContext.executeQuery(builder.build()).asFlow().map(::translate)

  companion object {
    fun forStructs(statement: Statement): BaseSpannerReader<Struct> {
      return object : BaseSpannerReader<Struct>() {
        override val builder: Statement.Builder = statement.toBuilder()
        override suspend fun translate(struct: Struct): Struct = struct
      }
    }
  }
}
