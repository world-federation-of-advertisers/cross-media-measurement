/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.access.common

import com.google.protobuf.ByteString
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap

/** Mapping for TLS client principals. */
class TlsClientPrincipalMapping(config: AuthorityKeyToPrincipalMap) {
  data class TlsClient(
    /** ID of the Principal resource. */
    val principalResourceId: String,
    /** Name of the resource protected by the Policy. */
    val protectedResourceNames: Set<String>,
    /** Authority key identifier (AKID) key ID of the certificate. */
    val authorityKeyIdentifier: ByteString,
  )

  private val clientsByPrincipalResourceId: Map<String, TlsClient>
  private val clientsByAuthorityKeyIdentifier: Map<ByteString, TlsClient>

  init {
    val clients =
      config.entriesList.map {
        val protectedResourceName = it.principalResourceName
        val principalResourceId =
          protectedResourceName.replace("/", "-").replace("_", "-").takeLast(63)
        check(ResourceIds.RFC_1034_REGEX.matches(principalResourceId)) {
          "Invalid character in protected resource name $protectedResourceName"
        }
        TlsClient(
          principalResourceId,
          setOf(protectedResourceName, ROOT_RESOURCE_NAME),
          it.authorityKeyIdentifier,
        )
      }

    clientsByPrincipalResourceId = clients.associateBy(TlsClient::principalResourceId)
    clientsByAuthorityKeyIdentifier = clients.associateBy(TlsClient::authorityKeyIdentifier)
  }

  /** Returns the [TlsClient] for the specified [principalResourceId], or `null` if not found. */
  fun getByPrincipalResourceId(principalResourceId: String): TlsClient? =
    clientsByPrincipalResourceId[principalResourceId]

  /** Returns the [TlsClient] for the specified [authorityKeyIdentifier], or `null` if not found. */
  fun getByAuthorityKeyIdentifier(authorityKeyIdentifier: ByteString): TlsClient? =
    clientsByAuthorityKeyIdentifier[authorityKeyIdentifier]

  companion object {
    /** Resource name used to indicate the API root. */
    private const val ROOT_RESOURCE_NAME = ""
  }
}
