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

package org.wfanet.measurement.integration.common

import java.security.MessageDigest
import java.time.LocalDate
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toLocalDate

/** Views of the high overlap dataset derived from its specs. */
object HighOverlapDataset {
  /** Resource path of the high overlap [ImpressionTestDataConfig]. */
  const val CONFIG_RESOURCE_PATH = "high_overlap_impression_test_data_config.textproto"

  /** EventGroup reference IDs by EDP, keyed `"${entityType}-${entityId}"`. */
  fun eventGroupRefIdsByEdp(config: ImpressionTestDataConfig): Map<String, Set<String>> =
    config.eventGroupsList
      .groupBy { it.edpName }
      .mapValues { (_, eventGroups) ->
        eventGroups
          .flatMap { eventGroup ->
            eventGroup.entityKeySpecsList.map { "${it.entityType}-${it.entityId}" }
          }
          .toSet()
      }

  /** Every date the specs cover, keyed by the EDP's `output_base_path`. */
  fun datesByImpressionPath(config: ImpressionTestDataConfig): Map<String, Set<LocalDate>> {
    val datesByPath = mutableMapOf<String, MutableSet<LocalDate>>()
    for (eventGroup in config.eventGroupsList) {
      val dates = datesByPath.getOrPut(eventGroup.outputBasePath) { mutableSetOf() }
      for (entityKeySpec in eventGroup.entityKeySpecsList) {
        val spec: SyntheticEventGroupSpec =
          ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
            entityKeySpec.dataSpecResourcePath
          )
        for (dateSpec in spec.dateSpecsList) {
          var date = dateSpec.dateRange.start.toLocalDate()
          val endExclusive = dateSpec.dateRange.endExclusive.toLocalDate()
          while (date.isBefore(endExclusive)) {
            dates.add(date)
            date = date.plusDays(1)
          }
        }
      }
    }
    return datesByPath
  }

  /**
   * Fingerprint of the spec files rooted at [configResourcePath].
   *
   * See `HighOverlapDatasetManifest.spec_fingerprint` for the algorithm.
   */
  fun fingerprint(configResourcePath: String): String {
    val configFile = ImpressionTestDataConfigs.resolveSpecPath(configResourcePath)
    val config = parseTextProto(configFile, ImpressionTestDataConfig.getDefaultInstance())
    val dataSpecPaths: Set<String> =
      config.eventGroupsList
        .flatMap { eventGroup ->
          eventGroup.entityKeySpecsList.map { it.dataSpecResourcePath } +
            eventGroup.dataSpecResourcePath
        }
        .filter { it.isNotEmpty() }
        .toSortedSet()

    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(configFile.readBytes())
    digest.update(
      ImpressionTestDataConfigs.resolveSpecPath(config.populationSpecResourcePath).readBytes()
    )
    for (path in dataSpecPaths) {
      digest.update(ImpressionTestDataConfigs.resolveSpecPath(path).readBytes())
    }
    return digest.digest().joinToString("") { "%02x".format(it) }
  }

  /** Blob key of the manifest describing what was seeded for [modelLineId]. */
  fun manifestBlobKey(modelLineId: String): String =
    "high-overlap/$modelLineId/dataset-manifest.binpb"
}
