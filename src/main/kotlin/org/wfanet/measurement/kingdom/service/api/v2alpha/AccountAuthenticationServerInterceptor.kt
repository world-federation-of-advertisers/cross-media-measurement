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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import io.grpc.BindableService
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.AccountConstants
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.authenticateAccountRequest

/** gRPC [ServerInterceptor] to check [Account] credentials coming in from a request. */
class AccountAuthenticationServerInterceptor(
  private val internalAccountsClient: AccountsCoroutineStub,
  private val redirectUri: String,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : SuspendableServerInterceptor(coroutineContext) {

  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    var context = Context.current()
    val idToken =
      headers.get(AccountConstants.ID_TOKEN_METADATA_KEY) ?: return next.startCall(call, headers)
    context = context.withValue(AccountConstants.CONTEXT_ID_TOKEN_KEY, idToken)

    try {
      val account = authenticateAccountCredentials(idToken)
      context =
        context
          .withPrincipal(AccountPrincipal(AccountKey(externalIdToApiId(account.externalAccountId))))
          .withValue(AccountConstants.CONTEXT_ACCOUNT_KEY, account)
    } catch (e: GeneralSecurityException) {
      call.close(Status.UNAUTHENTICATED.withCause(e), headers)
    } catch (e: StatusException) {
      val status =
        when (e.status.code) {
          Status.Code.NOT_FOUND -> {
            // The request might not require authentication, so this is fine.
            Status.OK
          }
          Status.Code.CANCELLED -> Status.CANCELLED
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }
      if (!status.isOk) {
        call.close(status, headers)
      }
    }

    return Contexts.interceptCall(context, call, headers, next)
  }

  private suspend fun authenticateAccountCredentials(idToken: String): Account {
    val openIdConnectIdentity =
      AccountsService.validateIdToken(
        idToken = idToken,
        redirectUri = redirectUri,
        internalAccountsStub = internalAccountsClient,
      )

    return internalAccountsClient.authenticateAccount(
      authenticateAccountRequest { identity = openIdConnectIdentity }
    )
  }
}

fun BindableService.withAccountAuthenticationServerInterceptor(
  internalAccountsClient: AccountsCoroutineStub,
  redirectUri: String,
): ServerServiceDefinition =
  ServerInterceptors.intercept(
    this,
    AccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri),
  )

fun ServerServiceDefinition.withAccountAuthenticationServerInterceptor(
  internalAccountsClient: AccountsCoroutineStub,
  redirectUri: String,
): ServerServiceDefinition =
  ServerInterceptors.intercept(
    this,
    AccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri),
  )

/** Executes [block] with [Account] installed in a new [Context]. */
fun <T> withAccount(account: Account, block: () -> T): T {
  return Context.current().withAccount(account).call(block)
}

fun Context.withAccount(account: Account): Context {
  return withValue(AccountConstants.CONTEXT_ACCOUNT_KEY, account)
}
