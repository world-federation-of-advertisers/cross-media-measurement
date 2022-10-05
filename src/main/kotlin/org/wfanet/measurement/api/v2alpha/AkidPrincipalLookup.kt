/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.common.api.AkidConfigPrincipalLookup
import org.wfanet.measurement.common.api.AkidConfigResourceNameLookup
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap

/** [PrincipalLookup] of [MeasurementPrincipal] by authority key identifier (AKID). */
class AkidPrincipalLookup(config: AuthorityKeyToPrincipalMap) :
  PrincipalLookup<MeasurementPrincipal, ByteString> {

  /**
   * Constructs [AkidConfigPrincipalLookup] from a file.
   *
   * @param textProto a [File] containing an [AuthorityKeyToPrincipalMap] message in text format
   */
  constructor(
    textProto: File
  ) : this(parseTextProto(textProto, AuthorityKeyToPrincipalMap.getDefaultInstance()))

  private val configLookup =
    object : AkidConfigPrincipalLookup<MeasurementPrincipal>(AkidConfigResourceNameLookup(config)) {

      override suspend fun getPrincipal(resourceName: String): MeasurementPrincipal? {
        return MeasurementPrincipal.fromName(resourceName)
      }
    }

  override suspend fun getPrincipal(lookupKey: ByteString): MeasurementPrincipal? {
    return configLookup.getPrincipal(lookupKey) ?: getDuchyPrincipal(lookupKey)
  }

  private fun getDuchyPrincipal(lookupKey: ByteString): DuchyPrincipal? {
    val entry: DuchyInfo.Entry = DuchyInfo.getByRootCertificateSkid(lookupKey) ?: return null
    return DuchyPrincipal(DuchyKey(entry.duchyId))
  }
}
