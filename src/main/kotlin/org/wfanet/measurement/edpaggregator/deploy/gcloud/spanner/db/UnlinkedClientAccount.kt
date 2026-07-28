/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.edpaggregator.ListUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccount
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.unlinkedClientAccount

private const val BASE_SQL =
  """
  SELECT
    DataProviderResourceId,
    ClientAccountReferenceId,
    Brands,
    EventGroupReferenceId,
    FirstObservedTime
  FROM
    UnlinkedClientAccounts
  """

/**
 * Reads all [UnlinkedClientAccount] rows for the specified DataProvider, ordered by reference ID.
 *
 * This is unpaginated and intended for the reconcile read-before-write; use
 * [readUnlinkedClientAccounts] with a limit for the paginated List read path.
 */
fun AsyncDatabaseClient.ReadContext.readAllUnlinkedClientAccounts(
  dataProviderResourceId: String
): Flow<UnlinkedClientAccount> {
  val sql = buildString {
    appendLine(BASE_SQL.trimIndent())
    appendLine("WHERE DataProviderResourceId = @dataProviderResourceId")
    appendLine("ORDER BY ClientAccountReferenceId ASC")
  }

  val query = statement(sql) { bind("dataProviderResourceId").to(dataProviderResourceId) }

  return executeQuery(query, Options.tag("action=readAllUnlinkedClientAccounts")).map {
    buildUnlinkedClientAccount(it)
  }
}

/** Reads a page of [UnlinkedClientAccount] rows ordered by reference ID. */
fun AsyncDatabaseClient.ReadContext.readUnlinkedClientAccounts(
  dataProviderResourceId: String,
  limit: Int,
  after: ListUnlinkedClientAccountsPageToken.After? = null,
): Flow<UnlinkedClientAccount> {
  val sql = buildString {
    appendLine(BASE_SQL.trimIndent())
    appendLine("WHERE DataProviderResourceId = @dataProviderResourceId")
    if (after != null) {
      appendLine("AND ClientAccountReferenceId > @afterClientAccountReferenceId")
    }
    appendLine("ORDER BY ClientAccountReferenceId ASC")
    appendLine("LIMIT @limit")
  }

  val query =
    statement(sql) {
      bind("dataProviderResourceId").to(dataProviderResourceId)
      bind("limit").to(limit.toLong())
      if (after != null) {
        bind("afterClientAccountReferenceId").to(after.clientAccountReferenceId)
      }
    }

  return executeQuery(query, Options.tag("action=readUnlinkedClientAccounts")).map {
    buildUnlinkedClientAccount(it)
  }
}

/** Buffers an insert mutation for a single [UnlinkedClientAccount] row. */
private fun AsyncDatabaseClient.TransactionContext.insertUnlinkedClientAccount(
  dataProviderResourceId: String,
  unlinkedClientAccount: UnlinkedClientAccount,
) {
  bufferInsertMutation("UnlinkedClientAccounts") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("ClientAccountReferenceId").to(unlinkedClientAccount.clientAccountReferenceId)
    set("Brands").toStringArray(unlinkedClientAccount.brandsList)
    set("EventGroupReferenceId").to(unlinkedClientAccount.eventGroupReferenceId)
    set("FirstObservedTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/**
 * Buffers an update mutation for the mutable fields of an [UnlinkedClientAccount] row.
 *
 * The `FirstObservedTime` column is intentionally not set, preserving the original value.
 */
private fun AsyncDatabaseClient.TransactionContext.updateUnlinkedClientAccount(
  dataProviderResourceId: String,
  unlinkedClientAccount: UnlinkedClientAccount,
) {
  bufferUpdateMutation("UnlinkedClientAccounts") {
    set("DataProviderResourceId").to(dataProviderResourceId)
    set("ClientAccountReferenceId").to(unlinkedClientAccount.clientAccountReferenceId)
    set("Brands").toStringArray(unlinkedClientAccount.brandsList)
    set("EventGroupReferenceId").to(unlinkedClientAccount.eventGroupReferenceId)
  }
}

/**
 * Reconciles the stored set of [UnlinkedClientAccount] rows for [dataProviderResourceId] against
 * [incoming] within a single transaction.
 * * Accounts in [incoming] that are not yet stored are inserted, stamping `first_observed_time`
 *   with the commit timestamp.
 * * Accounts in [incoming] that are already stored are kept, preserving their existing
 *   `first_observed_time`; their `brands` and `event_group_reference_id` are updated.
 * * Stored accounts that are absent from [incoming] are deleted.
 *
 * @return the reconciled set of [UnlinkedClientAccount]s. Newly-inserted accounts have their
 *   `first_observed_time` cleared; the caller is responsible for populating it from the commit
 *   timestamp.
 */
suspend fun AsyncDatabaseClient.TransactionContext.replaceUnlinkedClientAccounts(
  dataProviderResourceId: String,
  incoming: List<UnlinkedClientAccount>,
): List<UnlinkedClientAccount> {
  val existingByReferenceId: Map<String, UnlinkedClientAccount> =
    readAllUnlinkedClientAccounts(dataProviderResourceId).toList().associateBy {
      it.clientAccountReferenceId
    }
  val incomingReferenceIds: Set<String> = incoming.map { it.clientAccountReferenceId }.toSet()

  // Delete stored accounts that are absent from the incoming set (they have been linked).
  for (referenceId in existingByReferenceId.keys) {
    if (referenceId !in incomingReferenceIds) {
      buffer(Mutation.delete("UnlinkedClientAccounts", Key.of(dataProviderResourceId, referenceId)))
    }
  }

  return incoming.map { account ->
    val existing: UnlinkedClientAccount? = existingByReferenceId[account.clientAccountReferenceId]
    if (existing != null) {
      updateUnlinkedClientAccount(dataProviderResourceId, account)
      account.copy {
        this.dataProviderResourceId = dataProviderResourceId
        firstObservedTime = existing.firstObservedTime
      }
    } else {
      insertUnlinkedClientAccount(dataProviderResourceId, account)
      account.copy {
        this.dataProviderResourceId = dataProviderResourceId
        clearFirstObservedTime()
      }
    }
  }
}

private fun buildUnlinkedClientAccount(struct: Struct): UnlinkedClientAccount {
  return unlinkedClientAccount {
    dataProviderResourceId = struct.getString("DataProviderResourceId")
    clientAccountReferenceId = struct.getString("ClientAccountReferenceId")
    if (!struct.isNull("Brands")) {
      brands += struct.getStringList("Brands")
    }
    if (!struct.isNull("EventGroupReferenceId")) {
      eventGroupReferenceId = struct.getString("EventGroupReferenceId")
    }
    firstObservedTime = struct.getTimestamp("FirstObservedTime").toProto()
  }
}
