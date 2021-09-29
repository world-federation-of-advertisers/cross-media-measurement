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

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.SpannerException
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.usernameIdentity
import org.wfanet.measurement.internal.kingdom.UsernameCredentials
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UsernameIdentityReader

/**
 * Replace an existing account identity with a new username identity in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.ACCOUNT_NOT_FOUND]
 * * [KingdomInternalException.Code.USERNAME_ALREADY_EXISTS]
 * * [KingdomInternalException.Code.ACCOUNT_NOT_ACTIVATED]
 */
class ReplaceAccountIdentityWithNewUsernameIdentity(
  private val externalAccountId: Long,
  private val usernameCredentials: UsernameCredentials
) : SimpleSpannerWriter<Account>() {

  override suspend fun TransactionScope.runTransaction(): Account {
    val readAccountResult = readAccount()

    if (readAccountResult.account.activationState == Account.ActivationState.UNACTIVATED) {
      throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_ACTIVATED)
    }

    // if the existing account identity is a username identity
    UsernameIdentityReader()
      .readByAccountId(transactionContext, InternalId(readAccountResult.accountId))
      ?.let { updateUsernameIdentity(InternalId(it.usernameIdentityId), usernameCredentials) }
    // if the existing account identity is an open id connect identity
    ?: throw KingdomInternalException(
        KingdomInternalException.Code.ACCOUNT_NOT_ACTIVATED
      ) // TODO("open id support not yet implemented")

    return readAccountResult.account.copy {
      usernameIdentity = usernameIdentity { username = usernameCredentials.username }
    }
  }

  override suspend fun handleSpannerException(e: SpannerException): Account? {
    when (e.errorCode) {
      ErrorCode.ALREADY_EXISTS ->
        throw KingdomInternalException(KingdomInternalException.Code.USERNAME_ALREADY_EXISTS)
      else -> throw e
    }
  }

  private suspend fun TransactionScope.readAccount(): AccountReader.Result =
    AccountReader(Account.View.BASIC)
      .readByExternalAccountId(transactionContext, ExternalId(externalAccountId))
      ?: throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_FOUND)
}
