// Copyright 2026 The Cross-Media Measurement Authors
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

/** Resolves the authoritative version, allowing explicit configuration for historical lines. */
object ModelLineEventTemplateVersions {
  fun resolve(modelLine: ModelLine, configuredVersion: Int?): Int {
    require(configuredVersion == null || configuredVersion > 0) {
      "Configured event_template_version must be positive for ${modelLine.name}"
    }
    if (modelLine.hasEventTemplateVersion()) {
      val version = modelLine.eventTemplateVersion
      require(version > 0) {
        "Repository event_template_version must be positive for ${modelLine.name}"
      }
      require(configuredVersion == null || configuredVersion == version) {
        "Configured event_template_version does not match repository version for ${modelLine.name}"
      }
      return version
    }
    return requireNotNull(configuredVersion) {
      "event_template_version missing from repository and local configuration for ${modelLine.name}"
    }
  }
}
