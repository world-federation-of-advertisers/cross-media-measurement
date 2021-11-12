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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerOwnerReader

/**
 * Creates an account in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.ACCOUNT_NOT_OWNER]
 * * [KingdomInternalException.Code.ACCOUNT_NOT_FOUND]
 */
class CreateAccount(
  private val externalCreatorAccountId: Long,
  private val externalOwnedMeasurementConsumerId: Long
) : SimpleSpannerWriter<Account>() {

  override suspend fun TransactionScope.runTransaction(): Account {
    val internalAccountId = idGenerator.generateInternalId()
    val externalAccountId = idGenerator.generateExternalId()
    val activationToken = idGenerator.generateExternalId()

    val params = this@CreateAccount
    return account {
      transactionContext.bufferInsertMutation("Accounts") {
        if (params.externalCreatorAccountId != 0L) {
          val readCreatorAccountResult = readAccount(params.externalCreatorAccountId)
          set("CreatorAccountId" to readCreatorAccountResult.accountId)

          measurementConsumerCreationToken =
            readCreatorAccountResult.account.measurementConsumerCreationToken

          if (params.externalOwnedMeasurementConsumerId != 0L) {
            MeasurementConsumerOwnerReader()
              .checkOwnershipExist(
                transactionContext,
                InternalId(readCreatorAccountResult.accountId),
                ExternalId(params.externalOwnedMeasurementConsumerId)
              )
              ?: throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_OWNER)

            externalOwnedMeasurementConsumerId = params.externalOwnedMeasurementConsumerId
            set("OwnedMeasurementConsumerId" to params.externalOwnedMeasurementConsumerId)
          }
        } else {
          measurementConsumerCreationToken = idGenerator.generateExternalId().value
        }
        set("AccountId" to internalAccountId)
        set("ExternalAccountId" to externalAccountId)
        set("ActivationState" to Account.ActivationState.UNACTIVATED)
        set("ActivationToken" to activationToken)
        set("MeasurementConsumerCreationToken" to measurementConsumerCreationToken)
        set("CreateTime" to Value.COMMIT_TIMESTAMP)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      }

      this.externalAccountId = externalAccountId.value
      externalCreatorAccountId = params.externalCreatorAccountId
      activationState = Account.ActivationState.UNACTIVATED
      this.activationToken = activationToken.value
    }
  }

  private suspend fun TransactionScope.readAccount(externalAccountId: Long): AccountReader.Result =
    AccountReader().readByExternalAccountId(transactionContext, ExternalId(externalAccountId))
      ?: throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_FOUND)
}
