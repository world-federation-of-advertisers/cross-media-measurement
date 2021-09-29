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

/**
 * Sets an account's activation state to ACTIVATED and creates a username identity for it in the
 * database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.USERNAME_ALREADY_EXISTS]
 * * [KingdomInternalException.Code.ACCOUNT_NOT_FOUND]
 * * [KingdomInternalException.Code.ACCOUNT_ALREADY_ACTIVATED]
 */
class ActivateAccount(
  private val externalAccountId: Long,
  private val usernameCredentials: UsernameCredentials
) : SimpleSpannerWriter<Account>() {
  override suspend fun TransactionScope.runTransaction(): Account {
    val readAccountResult = readAccount()
    if (readAccountResult.account.activationState == Account.ActivationState.ACTIVATED) {
      throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_ALREADY_ACTIVATED)
    }
    updateAccountActivationState(
      InternalId(readAccountResult.accountId),
      Account.ActivationState.ACTIVATED
    )
    createUsernameIdentity(
      InternalId(readAccountResult.accountId),
      usernameCredentials.username,
      usernameCredentials.password
    )

    return readAccountResult.account.copy {
      clearActivationParams()
      activationState = Account.ActivationState.ACTIVATED
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
    AccountReader(Account.View.FULL)
      .readByExternalAccountId(transactionContext, ExternalId(externalAccountId))
      ?: throw KingdomInternalException(KingdomInternalException.Code.ACCOUNT_NOT_FOUND)
}
