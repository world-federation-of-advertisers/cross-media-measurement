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
import io.grpc.StatusRuntimeException
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.AccountConstants
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.Principal
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.DeferredForwardingListener
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.authenticateAccountRequest

/** gRPC [ServerInterceptor] to check [Account] credentials coming in from a request. */
class AccountAuthenticationServerInterceptor(
  private val internalAccountsClient: AccountsCoroutineStub,
  private val redirectUri: String
) : ServerInterceptor {

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val idToken = headers.get(AccountConstants.ID_TOKEN_METADATA_KEY)

    var context = Context.current()

    // it might be a request that doesn't require an ID token so can't close it
    if (idToken == null) {
      return Contexts.interceptCall(context, call, headers, next)
    } else {
      context = context.withValue(AccountConstants.CONTEXT_ID_TOKEN_KEY, idToken)

      val deferredForwardingListener = DeferredForwardingListener<ReqT>()

      CoroutineScope(Dispatchers.IO).launch {
        try {
          val account = authenticateAccountCredentials(idToken)
          context =
            context
              .withPrincipal(
                Principal.Account(AccountKey(externalIdToApiId(account.externalAccountId)))
              )
              .withValue(AccountConstants.CONTEXT_ACCOUNT_KEY, account)
        } catch (e: Exception) {
          when (e) {
            // it might be a request that has an ID token but doesn't require authentication
            // so can't close it
            is StatusRuntimeException,
            is StatusException -> {}
            else ->
              call.close(
                Status.UNKNOWN.withDescription("Unknown error when authenticating"),
                headers
              )
          }
        }

        deferredForwardingListener.setDelegate(Contexts.interceptCall(context, call, headers, next))
      }

      return deferredForwardingListener
    }
  }

  private suspend fun authenticateAccountCredentials(idToken: String): Account {
    val openIdConnectIdentity =
      AccountsService.validateIdToken(
        idToken = idToken,
        redirectUri = redirectUri,
        internalAccountsStub = internalAccountsClient
      )

    return internalAccountsClient.authenticateAccount(
      authenticateAccountRequest { identity = openIdConnectIdentity }
    )
  }
}

fun <T : AbstractStub<T>> T.withIdToken(idToken: String? = null): T {
  val extraHeaders = Metadata()
  idToken?.let { extraHeaders.put(AccountConstants.ID_TOKEN_METADATA_KEY, it) }
  return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders))
}

fun BindableService.withAccountAuthenticationServerInterceptor(
  internalAccountsClient: AccountsCoroutineStub,
  redirectUri: String
): ServerServiceDefinition =
  ServerInterceptors.intercept(
    this,
    AccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri)
  )

fun ServerServiceDefinition.withAccountAuthenticationServerInterceptor(
  internalAccountsClient: AccountsCoroutineStub,
  redirectUri: String
): ServerServiceDefinition =
  ServerInterceptors.intercept(
    this,
    AccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri)
  )

/** Executes [block] with [Account] installed in a new [Context]. */
fun <T> withAccount(account: Account, block: () -> T): T {
  return Context.current().withAccount(account).call(block)
}

fun Context.withAccount(account: Account): Context {
  return withValue(AccountConstants.CONTEXT_ACCOUNT_KEY, account)
}
