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
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.UnlinkedClientAccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UnlinkedClientAccountReader

/**
 * Deletes an [UnlinkedClientAccount] from the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [UnlinkedClientAccountNotFoundException] UnlinkedClientAccount not found
 */
class DeleteUnlinkedClientAccount(
  private val externalDataProviderId: ExternalId,
  private val clientAccountReferenceId: String,
) : SimpleSpannerWriter<UnlinkedClientAccount>() {
  override suspend fun TransactionScope.runTransaction(): UnlinkedClientAccount {
    val result =
      UnlinkedClientAccountReader()
        .readByDataProviderAndReferenceId(
          transactionContext,
          externalDataProviderId,
          clientAccountReferenceId,
        )
        ?: throw UnlinkedClientAccountNotFoundException(
          externalDataProviderId,
          clientAccountReferenceId,
        )

    transactionContext.buffer(
      Mutation.delete(
        "UnlinkedClientAccounts",
        KeySet.singleKey(Key.of(result.dataProviderId.value, clientAccountReferenceId)),
      )
    )

    return result.unlinkedClientAccount
  }
}
