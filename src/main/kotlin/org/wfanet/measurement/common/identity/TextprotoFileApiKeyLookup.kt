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

package org.wfanet.measurement.common.identity

import java.io.File
import org.wfanet.measurement.api.v2alpha.PrincipalServerInterceptor.PrincipalLookup
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.MeasurementConsumerToApiKeyMap
import org.wfanet.measurement.config.measurementConsumerToApiKeyMap
import org.wfanet.measurement.reporting.service.api.v1alpha.MeasurementConsumerServerInterceptor.ApiKeyLookup

/**
 * [PrincipalLookup] that reads a mapping from a file.
 *
 * @param textprotoFile contains an [MeasurementConsumerToApiKeyMap] in textproto format.
 */
class TextprotoFileApiKeyLookup(textprotoFile: File) : ApiKeyLookup {
  private val map: Map<String, String> =
    parseTextProto(textprotoFile, measurementConsumerToApiKeyMap {}).entriesList.associate {
      it.measurementConsumerName to it.apiKey
    }

  override fun get(measurementConsumerName: String): String? {
    return map[measurementConsumerName]
  }
}
