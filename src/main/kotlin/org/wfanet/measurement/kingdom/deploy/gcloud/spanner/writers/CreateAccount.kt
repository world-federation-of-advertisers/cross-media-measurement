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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.activationParams
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerOwnerReader

/**
 * Creates an account in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND]
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

    // to determine whether or not to actually insert
    var addMeasurementConsumerOwnersMutation: Mutation? = null

    return account {
      transactionContext.bufferInsertMutation("Accounts") {
        set("AccountId" to internalAccountId)
        set("ExternalAccountId" to externalAccountId)
        // if there is a creator account for this account
        if (this@CreateAccount.externalCreatorAccountId != 0L) {
          val readCreatorAccountResult = readAccount(this@CreateAccount.externalCreatorAccountId)
          set("CreatorAccountId" to readCreatorAccountResult.accountId)

          val measurementConsumerCreationToken =
            readCreatorAccountResult.account.measurementConsumerCreationToken
          this@account.measurementConsumerCreationToken = measurementConsumerCreationToken
          set(
            "MeasurementConsumerCreationToken" to
              ApiId(measurementConsumerCreationToken).externalId.value
          )

          if (this@CreateAccount.externalOwnedMeasurementConsumerId != 0L) {
            val ownedMeasurementConsumerId: InternalId =
              readMeasurementConsumerId(
                ExternalId(this@CreateAccount.externalOwnedMeasurementConsumerId)
              )
            if (MeasurementConsumerOwnerReader()
                .checkOwnershipExist(
                  transactionContext,
                  InternalId(readCreatorAccountResult.accountId),
                  ownedMeasurementConsumerId
                ) == null
            ) {
              throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_OWNER)
            }

            // builds the mutation for after the account is created
            addMeasurementConsumerOwnersMutation =
              insertMutation("MeasurementConsumerOwners") {
                set("AccountId" to internalAccountId)
                set("MeasurementConsumerId" to ownedMeasurementConsumerId)
              }

            set("OwnedMeasurementConsumerId" to ownedMeasurementConsumerId)
          }

          // for an account with no creator
        } else {
          val measurementConsumerCreationToken = idGenerator.generateExternalId()
          this@account.measurementConsumerCreationToken =
            measurementConsumerCreationToken.apiId.value
          set("MeasurementConsumerCreationToken" to measurementConsumerCreationToken)
        }
        set("ActivationState" to Account.ActivationState.UNACTIVATED)
        set("ActivationToken" to activationToken)
        set("CreateTime" to Value.COMMIT_TIMESTAMP)
        set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      }

      addMeasurementConsumerOwnersMutation?.bufferTo(transactionContext)

      this.externalAccountId = externalAccountId.value
      externalCreatorAccountId = this@CreateAccount.externalCreatorAccountId
      activationState = Account.ActivationState.UNACTIVATED
      activationParams =
        activationParams {
          externalOwnedMeasurementConsumerId = this@CreateAccount.externalOwnedMeasurementConsumerId
          this.activationToken = activationToken.apiId.value
        }
    }
  }

  private suspend fun TransactionScope.readAccount(externalAccountId: Long): AccountReader.Result =
    AccountReader(Account.View.FULL)
      .readByExternalAccountId(transactionContext, ExternalId(externalAccountId))
      ?: throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_FOUND)

  private suspend fun TransactionScope.readMeasurementConsumerId(
    externalMeasurementConsumerId: ExternalId
  ): InternalId {
    val column = "MeasurementConsumerId"
    return transactionContext.readRowUsingIndex(
        "MeasurementConsumers",
        "MeasurementConsumersByExternalId",
        Key.of(externalMeasurementConsumerId.value),
        column
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw KingdomInternalException(
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
      )
  }
}
