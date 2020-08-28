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

package org.wfanet.measurement.db.gcp

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

/**
 * Dispatcher for blocking database operations.
 */
fun spannerDispatcher(): CoroutineDispatcher = Dispatchers.IO

/**
 * Executes a RMW transaction.
 *
 * This wraps the Java API to be more convenient for Kotlin because the Java API is nullable but the
 * block given isn't, so coercion from nullable to not is needed.
 *
 * @param block the body of the transaction
 * @return the result of [block]
 */
fun <T> DatabaseClient.runReadWriteTransaction(block: (TransactionContext) -> T): T =
  readWriteTransaction().run(block)!!

/**
 * Convenience function for appending without worrying about whether the last [append] had
 * sufficient whitespace -- this adds a newline before and a space after.
 */
fun Statement.Builder.appendClause(sql: String): Statement.Builder = append("\n$sql ")
