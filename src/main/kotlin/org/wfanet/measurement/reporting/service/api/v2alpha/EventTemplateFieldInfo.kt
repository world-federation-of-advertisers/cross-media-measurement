/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.Descriptors
import org.wfanet.measurement.api.v2alpha.MediaType

object EventTemplateFieldInfo {
  data class SupportedReportingFeatures(
    var groupable: Boolean = false,
    var filterable: Boolean = false,
    var impressionQualification: Boolean = false,
  )

  data class EventTemplateFieldInfo(
    val mediaType: MediaType,
    val isPopulationAttribute: Boolean,
    val supportedReportingFeatures: SupportedReportingFeatures,
    val type: Descriptors.FieldDescriptor.Type,
    // Only valid if type is MESSAGE
    val messageTypeFullName: String,
    // Map of Enum name to Enum number
    val enumValuesMap: Map<String, Int>,
  )
}
