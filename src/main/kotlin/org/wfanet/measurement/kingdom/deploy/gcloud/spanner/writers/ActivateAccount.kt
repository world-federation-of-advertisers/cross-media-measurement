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
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountActivationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuplicateAccountIdentityException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PermissionDeniedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdConnectIdentityReader

/**
 * Sets an account's activation state to ACTIVATED and creates an open id identity for it in the
 * database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [DuplicateAccountIdentityException] Duplicated Account identity with the same issuer and
 *   subject
 * @throws [AccountNotFoundException] Account not found
 * @throws [AccountActivationStateIllegalException] Account activation state is already ACTIVATED
 * @throws [MeasurementConsumerNotFoundException] Owned Measurement Consumer not found
 */
class ActivateAccount(
  private val externalAccountId: ExternalId,
  private val activationToken: ExternalId,
  private val issuer: String,
  private val subject: String,
) : SimpleSpannerWriter<Account>() {
  override suspend fun TransactionScope.runTransaction(): Account {
    if (isIdentityDuplicate(issuer = issuer, subject = subject)) {
      throw DuplicateAccountIdentityException(externalAccountId, issuer, subject)
    }

    val readAccountResult = readAccount(externalAccountId)

    if (readAccountResult.account.activationToken != activationToken.value) {
      throw PermissionDeniedException()
    }

    if (readAccountResult.account.activationState == Account.ActivationState.ACTIVATED) {
      throw AccountActivationStateIllegalException(
        externalAccountId,
        Account.ActivationState.ACTIVATED
      )
    }

    val internalOpenIdConnectIdentityId = idGenerator.generateInternalId()
    transactionContext.bufferInsertMutation("OpenIdConnectIdentities") {
      set("OpenIdConnectIdentityId" to internalOpenIdConnectIdentityId)
      set("AccountId" to readAccountResult.accountId)
      set("Issuer" to issuer)
      set("Subject" to subject)
    }

    transactionContext.bufferUpdateMutation("Accounts") {
      set("AccountId" to readAccountResult.accountId)
      set("ActivationState" to Account.ActivationState.ACTIVATED)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    if (readAccountResult.account.externalOwnedMeasurementConsumerId != 0L) {
      val ownedMeasurementConsumerId =
        readMeasurementConsumerId(readAccountResult.account.externalOwnedMeasurementConsumerId)

      transactionContext.bufferInsertMutation("MeasurementConsumerOwners") {
        set("MeasurementConsumerId" to ownedMeasurementConsumerId)
        set("AccountId" to readAccountResult.accountId)
      }
    }

    val source = this@ActivateAccount
    return readAccountResult.account.copy {
      activationState = Account.ActivationState.ACTIVATED
      openIdIdentity = openIdConnectIdentity {
        issuer = source.issuer
        subject = source.subject
      }
      externalOwnedMeasurementConsumerIds +=
        readAccountResult.account.externalOwnedMeasurementConsumerId
    }
  }

  private suspend fun TransactionScope.isIdentityDuplicate(
    issuer: String,
    subject: String,
  ): Boolean =
    OpenIdConnectIdentityReader()
      .readByIssuerAndSubject(transactionContext, issuer = issuer, subject = subject)
      .let {
        return it != null
      }

  private suspend fun TransactionScope.readAccount(
    externalAccountId: ExternalId
  ): AccountReader.Result =
    AccountReader().readByExternalAccountId(transactionContext, externalAccountId)
      ?: throw AccountNotFoundException(externalAccountId)

  private suspend fun TransactionScope.readMeasurementConsumerId(
    externalMeasurementConsumerId: Long
  ): Long =
    MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(
        transactionContext,
        ExternalId(externalMeasurementConsumerId)
      )
      ?.measurementConsumerId
      ?: throw MeasurementConsumerNotFoundException(ExternalId(externalMeasurementConsumerId))
}
