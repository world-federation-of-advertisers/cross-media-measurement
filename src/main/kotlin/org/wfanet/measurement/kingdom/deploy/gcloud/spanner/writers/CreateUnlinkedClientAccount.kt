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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountAlreadyExistsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UnlinkedClientAccountReader

/**
 * Creates an [UnlinkedClientAccount] in the database.
 *
 * Throws a subclass of
 * [org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException] on
 * [execute].
 *
 * @throws [DataProviderNotFoundException] when the DataProvider is not found
 * @throws [UnlinkedClientAccountAlreadyExistsException] when an UnlinkedClientAccount with the same
 *   reference ID already exists for the DataProvider
 */
class CreateUnlinkedClientAccount(private val unlinkedClientAccount: UnlinkedClientAccount) :
  SpannerWriter<UnlinkedClientAccount, UnlinkedClientAccount>() {
  override suspend fun TransactionScope.runTransaction(): UnlinkedClientAccount {
    val externalDataProviderId = ExternalId(unlinkedClientAccount.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)
    val dataProviderId = InternalId(dataProviderResult.dataProviderId)

    val existing =
      UnlinkedClientAccountReader()
        .readByDataProviderAndReferenceId(
          transactionContext,
          externalDataProviderId,
          unlinkedClientAccount.clientAccountReferenceId,
        )
    if (existing != null) {
      throw UnlinkedClientAccountAlreadyExistsException(
        externalDataProviderId,
        unlinkedClientAccount.clientAccountReferenceId,
      )
    }

    val existingClientAccount =
      ClientAccountReader()
        .readByDataProviderAndReferenceId(
          transactionContext,
          externalDataProviderId,
          unlinkedClientAccount.clientAccountReferenceId,
        )
    if (existingClientAccount != null) {
      throw ClientAccountAlreadyExistsException(
        externalDataProviderId,
        unlinkedClientAccount.clientAccountReferenceId,
      )
    }

    transactionContext.bufferInsertMutation("UnlinkedClientAccounts") {
      set("DataProviderId").to(dataProviderId.value)
      set("ClientAccountReferenceId").to(unlinkedClientAccount.clientAccountReferenceId)
      if (unlinkedClientAccount.hasEntityMetadata()) {
        set("EntityMetadata").to(unlinkedClientAccount.entityMetadata)
      }
      setObservedEventGroupColumns(unlinkedClientAccount)
      set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    }

    return unlinkedClientAccount.copy { this.externalDataProviderId = externalDataProviderId.value }
  }

  override fun ResultScope<UnlinkedClientAccount>.buildResult(): UnlinkedClientAccount {
    return checkNotNull(transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}

/**
 * Sets the EventGroup traceability columns from the `observed_event_group` oneof.
 *
 * Exactly one of the traceability columns is populated per the observed_event_group oneof: an
 * entity key populates the EventGroupEntityKey* columns, an event group reference ID populates
 * EventGroupReferenceId, and an unset oneof leaves all three null so the unset case round-trips as
 * unset rather than an empty reference ID.
 */
internal fun Mutation.WriteBuilder.setObservedEventGroupColumns(account: UnlinkedClientAccount) {
  when (account.observedEventGroupCase) {
    UnlinkedClientAccount.ObservedEventGroupCase.ENTITY_KEY -> {
      set("EventGroupReferenceId").to(null as String?)
      set("EventGroupEntityKeyType").to(account.entityKey.entityType)
      set("EventGroupEntityKeyId").to(account.entityKey.entityId)
    }
    UnlinkedClientAccount.ObservedEventGroupCase.EVENT_GROUP_REFERENCE_ID -> {
      set("EventGroupReferenceId").to(account.eventGroupReferenceId)
      set("EventGroupEntityKeyType").to(null as String?)
      set("EventGroupEntityKeyId").to(null as String?)
    }
    UnlinkedClientAccount.ObservedEventGroupCase.OBSERVEDEVENTGROUP_NOT_SET -> {
      set("EventGroupReferenceId").to(null as String?)
      set("EventGroupEntityKeyType").to(null as String?)
      set("EventGroupEntityKeyId").to(null as String?)
    }
  }
}
