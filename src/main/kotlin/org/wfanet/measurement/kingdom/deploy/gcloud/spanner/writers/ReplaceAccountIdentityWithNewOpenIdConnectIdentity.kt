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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdConnectIdentityReader

/**
 * Replace an existing account identity with a new username identity in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [ErrorCode.ACCOUNT_NOT_FOUND]
 * * [ErrorCode.DUPLICATE_ACCOUNT_IDENTITY]
 * * [ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL]
 */
class ReplaceAccountIdentityWithNewOpenIdConnectIdentity(
  private val externalAccountId: ExternalId,
  private val issuer: String,
  private val subject: String,
) : SimpleSpannerWriter<Account>() {

  override suspend fun TransactionScope.runTransaction(): Account {
    if (isIdentityDuplicate(issuer = issuer, subject = subject)) {
      throw KingdomInternalException(ErrorCode.DUPLICATE_ACCOUNT_IDENTITY)
    }

    val readAccountResult = readAccount(externalAccountId)

    if (readAccountResult.account.activationState == Account.ActivationState.UNACTIVATED) {
      throw KingdomInternalException(ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL)
    }

    OpenIdConnectIdentityReader()
      .readByAccountId(transactionContext, readAccountResult.accountId)
      ?.let {
        transactionContext.bufferUpdateMutation("OpenIdConnectIdentities") {
          set("OpenIdConnectIdentityId" to it.openIdConnectIdentityId)
          set("AccountId" to it.accountId)
          set("Issuer" to issuer)
          set("Subject" to subject)
        }
      }
      ?: throw KingdomInternalException(ErrorCode.ACCOUNT_ACTIVATION_STATE_ILLEGAL)

    val source = this@ReplaceAccountIdentityWithNewOpenIdConnectIdentity
    return readAccountResult.account.copy {
      openIdIdentity =
        AccountKt.openIdConnectIdentity {
          issuer = source.issuer
          subject = source.subject
        }
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
      ?: throw KingdomInternalException(ErrorCode.ACCOUNT_NOT_FOUND)
}
