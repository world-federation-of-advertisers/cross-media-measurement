// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap
import org.wfanet.measurement.reporting.service.api.v1alpha.PrincipalServerInterceptor.PrincipalLookup

/**
 * [PrincipalLookup] that reads a mapping from a file.
 *
 * @param textprotoFile contains an [AuthorityKeyToPrincipalMap] in textproto format.
 */
class TextprotoFilePrincipalLookup(
  textprotoFile: File,
  configLookup: PrincipalServerInterceptor.ConfigLookup
) : PrincipalLookup {
  private val map: Map<ByteString, MeasurementConsumerPrincipal> =
    parseTextProto(textprotoFile, AuthorityKeyToPrincipalMap.getDefaultInstance())
      .entriesList
      .associate { it.authorityKeyIdentifier to it.principalResourceName }
      .mapValues { MeasurementConsumerKey.fromName(it.value) }
      .filterNullValues()
      .mapValues { (_, resourceKey) ->
        val config =
          requireNotNull(configLookup.get(resourceKey.toName())) {
            "No config found for $resourceKey"
          }
        MeasurementConsumerPrincipal(resourceKey, config)
      }

  override fun get(authorityKeyIdentifier: ByteString): ReportingPrincipal? {
    return map[authorityKeyIdentifier]
  }
}

private fun <K, V> Map<K, V?>.filterNullValues(): Map<K, V> {
  @Suppress("UNCHECKED_CAST") return filterValues { it != null } as Map<K, V>
}
