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

import io.grpc.Metadata
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils

private const val KEY_NAME = "principal"
private val PRINCIPAL_NAME_METADATA_KEY: Metadata.Key<String> =
  Metadata.Key.of(KEY_NAME, Metadata.ASCII_STRING_MARSHALLER)

/**
 * Sets metadata key "principal" on all outgoing requests. Principal is [ResourceKey] for
 * DataProviders or ModelProviders. On the server side, use [MetadataPrincipalServerInterceptor].
 * Note that this should only be used in in-process tests where mTLS isn't used.
 *
 * Usage: val someStub =
 * SomeServiceCoroutineStub(channel).withPrincipalName("dataProviders/Ac8hsieOp")
 */
fun <T : AbstractStub<T>> T.withPrincipalName(name: String): T {
  val extraHeaders = Metadata()
  extraHeaders.put(PRINCIPAL_NAME_METADATA_KEY, name)
  return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders))
}
