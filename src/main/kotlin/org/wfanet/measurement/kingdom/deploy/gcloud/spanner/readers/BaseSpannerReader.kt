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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient

/** Base abstraction for reading from Spanner. */
abstract class BaseSpannerReader<T> {
  /** Provides a Statement builder for modification -- this is used to execute the query. */
  abstract val builder: Statement.Builder

  /** Transforms the results of the query. */
  abstract suspend fun translate(struct: Struct): T

  /** Executes the query. */
  fun execute(readContext: AsyncDatabaseClient.ReadContext): Flow<T> {
    logger.fine { "Executing Query: " + builder.build() }
    return readContext.executeQuery(builder.build()).map(::translate)
  }

  companion object {
    fun forStructs(statement: Statement): BaseSpannerReader<Struct> {
      return object : BaseSpannerReader<Struct>() {
        override val builder: Statement.Builder = statement.toBuilder()
        override suspend fun translate(struct: Struct): Struct = struct
      }
    }

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
