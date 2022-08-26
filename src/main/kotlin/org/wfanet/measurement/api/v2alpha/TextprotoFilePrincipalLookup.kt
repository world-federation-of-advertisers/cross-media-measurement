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

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.api.v2alpha.PrincipalServerInterceptor.PrincipalLookup
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap
import org.wfanet.measurement.config.authorityKeyToPrincipalMap

/**
 * [PrincipalLookup] that reads a mapping from a file.
 *
 * @param textprotoFile contains an [AuthorityKeyToPrincipalMap] in textproto format.
 */
class TextprotoFilePrincipalLookup(textprotoFile: File) : PrincipalLookup {
  private val map: Map<ByteString, MeasurementPrincipal> =
    parseTextProto(textprotoFile, authorityKeyToPrincipalMap {})
      .entriesList
      .associate { it.authorityKeyIdentifier to it.principalResourceName }
      .mapValues {
        requireNotNull(MeasurementPrincipal.fromName(it.value)) {
          "Invalid Principal name: ${it.value}"
        }
      }

  override fun get(authorityKeyIdentifier: ByteString): MeasurementPrincipal? {
    return map[authorityKeyIdentifier]
  }
}
