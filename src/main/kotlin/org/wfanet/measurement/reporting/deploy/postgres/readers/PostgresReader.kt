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

package org.wfanet.measurement.reporting.deploy.postgres.readers

import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import reactor.core.publisher.Mono

/** Abstraction for reading from Postgres. */
abstract class PostgresReader<T : Any> {
  protected abstract val baseSql: String

  protected val builder by lazy { StringBuilder(baseSql) }

  /** Transforms a R2DBC row into an instance of T. */
  protected abstract fun translate(row: Row): T

  /** Executes the query. */
  fun execute(statement: Statement): Flow<T> {
    logger.fine { "Executing Query: $builder" }
    val resultMono = Mono.from(statement.execute())

    return resultMono.flatMapMany { result -> result.map { row, _ -> translate(row) } }.asFlow()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
