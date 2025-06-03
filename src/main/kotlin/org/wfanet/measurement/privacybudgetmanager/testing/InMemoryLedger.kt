/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.privacybudgetmanager.testing

import org.wfanet.measurement.privacybudgetmanager.Ledger
import org.wfanet.measurement.privacybudgetmanager.LedgerRowKey
import org.wfanet.measurement.privacybudgetmanager.Query
import org.wfanet.measurement.privacybudgetmanager.Slice
import org.wfanet.measurement.privacybudgetmanager.TransactionContext

class InMemoryLedger : Ledger {
  override fun startTransaction(): TransactionContext = TODO("uakyol: implement this")
}

class TestInMemoryLedgerTransactionContext : TransactionContext {
  override suspend fun readQueries(queries: List<Query>, maxBatchSize: Int): List<Query> =
    TODO("uakyol: implement this")

  override suspend fun readChargeRows(rowKeys: List<LedgerRowKey>, maxBatchSize: Int): Slice =
    TODO("uakyol: implement this")

  override suspend fun write(delta: Slice, queries: List<Query>, maxBatchSize: Int): List<Query> =
    TODO("uakyol: implement this")

  override suspend fun commit() = TODO("uakyol: implement this")

  override fun close() = TODO("uakyol: implement this")
}
