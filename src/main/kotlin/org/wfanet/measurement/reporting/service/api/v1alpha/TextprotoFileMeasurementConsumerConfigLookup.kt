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

import java.io.File
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.reporting.service.api.v1alpha.PrincipalServerInterceptor.ConfigLookup

/**
 * [PrincipalLookup] that reads a mapping from a file.
 *
 * @param textprotoFile contains an [MeasurementConsumerConfigs] in textproto format.
 */
class TextprotoFileMeasurementConsumerConfigLookup(textprotoFile: File) : ConfigLookup {
  private val map: Map<String, MeasurementConsumerConfig> =
    parseTextProto(textprotoFile, MeasurementConsumerConfigs.getDefaultInstance()).configsMap

  override fun get(measurementConsumerName: String): MeasurementConsumerConfig? {
    return map[measurementConsumerName]
  }
}
