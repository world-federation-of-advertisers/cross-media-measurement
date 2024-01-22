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

package org.wfanet.measurement.common.api

import com.google.protobuf.ByteString
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap

/**
 * A [ResourceNameLookup] where the lookup key is the authority key identifier (AKID) of an X.509
 * certificate contained in [config].
 */
class AkidConfigResourceNameLookup(config: AuthorityKeyToPrincipalMap) :
  ResourceNameLookup<ByteString> {

  private val lookupTable: Map<ByteString, String> =
    config.entriesList.associateBy(
      AuthorityKeyToPrincipalMap.Entry::getAuthorityKeyIdentifier,
      AuthorityKeyToPrincipalMap.Entry::getPrincipalResourceName,
    )

  override suspend fun getResourceName(lookupKey: ByteString): String? = lookupTable[lookupKey]
}
