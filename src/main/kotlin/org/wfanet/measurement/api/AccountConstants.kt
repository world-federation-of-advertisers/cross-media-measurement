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

package org.wfanet.measurement.api

import io.grpc.Context
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.internal.kingdom.Account

object AccountConstants {
  /** Context key for an authenticated account. */
  val CONTEXT_ACCOUNT_KEY: Context.Key<Account> = Context.key("account")

  /** Context key for a provided id token. */
  val CONTEXT_ID_TOKEN_KEY: Context.Key<String> = Context.key("id_token")

  /** Metadata key for the id token for an [Account] with a [Account.OpenIdConnectIdentity]. */
  val ID_TOKEN_METADATA_KEY: Metadata.Key<String> =
    Metadata.Key.of("id_token", Metadata.ASCII_STRING_MARSHALLER)
}

val accountFromCurrentContext: Account
  get() =
    AccountConstants.CONTEXT_ACCOUNT_KEY.get()
      ?: failGrpc(Status.UNAUTHENTICATED) { "Account not found" }

fun <T : AbstractStub<T>> T.withIdToken(idToken: String? = null): T {
  val metadata = Metadata()
  idToken?.let { metadata.put(AccountConstants.ID_TOKEN_METADATA_KEY, it) }
  return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
}
