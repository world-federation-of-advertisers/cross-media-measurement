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

package org.wfanet.measurement.common.identity

import io.grpc.CallCredentials
import io.grpc.Metadata
import io.grpc.stub.AbstractStub
import java.util.concurrent.Executor

/**
 * Returns a new stub that uses [TrustedPrincipalCallCredentials].
 *
 * Usage: val someStub =
 * SomeServiceCoroutineStub(channel).withPrincipalName("dataProviders/Ac8hsieOp")
 */
fun <T : AbstractStub<T>> T.withPrincipalName(name: String): T =
  withCallCredentials(TrustedPrincipalCallCredentials(name))

/**
 * Trusted credentials for use with
 * [org.wfanet.measurement.api.v2alpha.testing.MetadataPrincipalServerInterceptor].
 */
class TrustedPrincipalCallCredentials(val name: String) : CallCredentials() {
  override fun applyRequestMetadata(
    requestInfo: RequestInfo,
    appExecutor: Executor,
    applier: MetadataApplier,
  ) {
    val headers = Metadata().apply { put(PRINCIPAL_NAME_METADATA_KEY, name) }
    applier.apply(headers)
  }

  companion object {
    private const val KEY_NAME = "x-trusted-principal-name"
    private val PRINCIPAL_NAME_METADATA_KEY: Metadata.Key<String> =
      Metadata.Key.of(KEY_NAME, Metadata.ASCII_STRING_MARSHALLER)

    fun fromHeaders(headers: Metadata): TrustedPrincipalCallCredentials? {
      val name = headers.get(PRINCIPAL_NAME_METADATA_KEY) ?: return null
      return TrustedPrincipalCallCredentials(name)
    }
  }
}
