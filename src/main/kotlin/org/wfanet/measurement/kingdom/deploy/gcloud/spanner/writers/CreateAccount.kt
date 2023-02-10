// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PermissionDeniedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerOwnerReader

/**
 * Creates an account in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [PermissionDeniedException] Permission denied due to ownership of MeasurementConsumer
 * @throws [AccountNotFoundException] Creator's Account not found
 */
class CreateAccount(
  private val externalCreatorAccountId: ExternalId?,
  private val externalOwnedMeasurementConsumerId: ExternalId?
) : SimpleSpannerWriter<Account>() {

  override suspend fun TransactionScope.runTransaction(): Account {
    val internalAccountId = idGenerator.generateInternalId()
    val externalAccountId = idGenerator.generateExternalId()
    val activationToken = idGenerator.generateExternalId()

    val source = this@CreateAccount
    return account {
      transactionContext.bufferInsertMutation("Accounts") {
        if (source.externalCreatorAccountId != null) {
          val readCreatorAccountResult = readAccount(source.externalCreatorAccountId)
          set("CreatorAccountId" to readCreatorAccountResult.accountId)

          externalCreatorAccountId = source.externalCreatorAccountId.value

          if (source.externalOwnedMeasurementConsumerId != null) {
            MeasurementConsumerOwnerReader()
              .checkOwnershipExist(
                transactionContext,
                readCreatorAccountResult.accountId,
                source.externalOwnedMeasurementConsumerId
              )
              ?.let {
                externalOwnedMeasurementConsumerId = source.externalOwnedMeasurementConsumerId.value
                set("OwnedMeasurementConsumerId" to it.measurementConsumerId)
              }
              ?: throw PermissionDeniedException()
          }
        }

        set("AccountId" to internalAccountId)
        set("ExternalAccountId" to externalAccountId)
        set("ActivationState" to Account.ActivationState.UNACTIVATED)
        set("ActivationToken" to activationToken)
        set("CreateTime" to Value.COMMIT_TIMESTAMP)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      }

      this.externalAccountId = externalAccountId.value
      activationState = Account.ActivationState.UNACTIVATED
      this.activationToken = activationToken.value
    }
  }

  private suspend fun TransactionScope.readAccount(
    externalAccountId: ExternalId
  ): AccountReader.Result =
    AccountReader().readByExternalAccountId(transactionContext, externalAccountId)
      ?: throw AccountNotFoundException(externalAccountId)
}
