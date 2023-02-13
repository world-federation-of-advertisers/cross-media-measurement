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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.api.AkidConfigPrincipalLookup
import org.wfanet.measurement.common.api.AkidConfigResourceNameLookup
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.api.toResourceKeyLookup
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs

/** [PrincipalLookup] of [ReportingPrincipal] by authority key identifier (AKID). */
class AkidPrincipalLookup(
  akidConfig: AuthorityKeyToPrincipalMap,
  measurementConsumerConfigs: MeasurementConsumerConfigs
) : PrincipalLookup<ReportingPrincipal, ByteString> {

  /**
   * Constructs [AkidConfigPrincipalLookup] from a file.
   *
   * @param akidConfig a [File] containing an [AuthorityKeyToPrincipalMap] message in text format
   * @param measurementConsumerConfigs a [File] containing a [MeasurementConsumerConfigs] message in
   *   text format
   */
  constructor(
    akidConfig: File,
    measurementConsumerConfigs: File
  ) : this(
    parseTextProto(akidConfig, AuthorityKeyToPrincipalMap.getDefaultInstance()),
    parseTextProto(measurementConsumerConfigs, MeasurementConsumerConfigs.getDefaultInstance())
  )

  private val measurementConsumerConfigs: Map<String, MeasurementConsumerConfig> =
    measurementConsumerConfigs.configsMap

  private val resourceKeyLookup =
    AkidConfigResourceNameLookup(akidConfig).toResourceKeyLookup(MeasurementConsumerKey.FACTORY)

  override suspend fun getPrincipal(lookupKey: ByteString): ReportingPrincipal? {
    val resourceKey: ResourceKey = resourceKeyLookup.getResourceKey(lookupKey) ?: return null
    return when (resourceKey) {
      is MeasurementConsumerKey -> {
        val resourceName: String = resourceKey.toName()
        val config =
          measurementConsumerConfigs[resourceName]
            ?: error("Missing MeasurementConsumerConfig for $resourceName")
        MeasurementConsumerPrincipal(resourceKey, config)
      }
      else -> null
    }
  }
}
