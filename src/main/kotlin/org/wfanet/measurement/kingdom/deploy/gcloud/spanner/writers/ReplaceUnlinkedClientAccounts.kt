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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UnlinkedClientAccountReader

/**
 * Reconciles the stored set of [UnlinkedClientAccount] rows for a DataProvider against [incoming]
 * within a single transaction.
 * * Accounts in [incoming] that are not yet stored are inserted, stamping `first_observed_time`
 *   with the commit timestamp.
 * * Accounts in [incoming] that are already stored are kept, preserving their existing
 *   `first_observed_time`; their `brands` and `observed_event_group` are updated.
 * * Stored accounts that are absent from [incoming] are deleted.
 *
 * Throws a subclass of
 * [org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException] on
 * [execute].
 *
 * @throws [DataProviderNotFoundException] when the DataProvider is not found
 */
class ReplaceUnlinkedClientAccounts(
  private val externalDataProviderId: ExternalId,
  private val incoming: List<UnlinkedClientAccount>,
) : SpannerWriter<List<UnlinkedClientAccount>, List<UnlinkedClientAccount>>() {
  override suspend fun TransactionScope.runTransaction(): List<UnlinkedClientAccount> {
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    val dataProviderId = InternalId(dataProviderResult.dataProviderId)
    val externalDataProviderIdValue = externalDataProviderId.value

    val existingByReferenceId: Map<String, UnlinkedClientAccountReader.Result> =
      UnlinkedClientAccountReader()
        .readByDataProviderId(transactionContext, dataProviderId)
        .associateBy { it.unlinkedClientAccount.clientAccountReferenceId }
    val incomingReferenceIds: Set<String> = incoming.map { it.clientAccountReferenceId }.toSet()

    // Delete stored accounts that are absent from the incoming set (they have been linked).
    for (referenceId in existingByReferenceId.keys) {
      if (referenceId !in incomingReferenceIds) {
        transactionContext.buffer(
          Mutation.delete("UnlinkedClientAccounts", Key.of(dataProviderId.value, referenceId))
        )
      }
    }

    return incoming.map { account ->
      val existing: UnlinkedClientAccountReader.Result? =
        existingByReferenceId[account.clientAccountReferenceId]
      if (existing != null) {
        transactionContext.bufferUpdateMutation("UnlinkedClientAccounts") {
          set("DataProviderId").to(dataProviderId.value)
          set("ClientAccountReferenceId").to(account.clientAccountReferenceId)
          set("Brands").toStringArray(account.brandsList)
          setObservedEventGroupColumns(account)
        }
        account.copy {
          externalDataProviderId = externalDataProviderIdValue
          firstObservedTime = existing.unlinkedClientAccount.firstObservedTime
        }
      } else {
        transactionContext.bufferInsertMutation("UnlinkedClientAccounts") {
          set("DataProviderId").to(dataProviderId.value)
          set("ClientAccountReferenceId").to(account.clientAccountReferenceId)
          set("Brands").toStringArray(account.brandsList)
          setObservedEventGroupColumns(account)
          set("FirstObservedTime").to(Value.COMMIT_TIMESTAMP)
        }
        account.copy {
          externalDataProviderId = externalDataProviderIdValue
          clearFirstObservedTime()
        }
      }
    }
  }

  override fun ResultScope<List<UnlinkedClientAccount>>.buildResult(): List<UnlinkedClientAccount> {
    val commitTime = commitTimestamp.toProto()
    return checkNotNull(transactionResult).map { account ->
      if (account.hasFirstObservedTime()) {
        account
      } else {
        account.copy { firstObservedTime = commitTime }
      }
    }
  }
}

/**
 * Sets the EventGroup traceability columns from the `observed_event_group` oneof.
 *
 * When [account] carries an entity key, the EventGroupEntityKey* columns are populated and
 * EventGroupReferenceId is left null; otherwise EventGroupReferenceId is populated and the
 * entity-key columns are left null.
 */
private fun Mutation.WriteBuilder.setObservedEventGroupColumns(account: UnlinkedClientAccount) {
  if (account.hasEntityKey()) {
    set("EventGroupReferenceId").to(null as String?)
    set("EventGroupEntityKeyType").to(account.entityKey.entityType)
    set("EventGroupEntityKeyId").to(account.entityKey.entityId)
  } else {
    set("EventGroupReferenceId").to(account.eventGroupReferenceId)
    set("EventGroupEntityKeyType").to(null as String?)
    set("EventGroupEntityKeyId").to(null as String?)
  }
}
