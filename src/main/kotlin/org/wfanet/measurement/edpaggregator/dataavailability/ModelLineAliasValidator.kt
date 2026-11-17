/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.dataavailability

import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.ModelLineCutoverValidator

/** Validates external alias ownership and internal model-line publication suppression. */
object ModelLineAliasValidator {
  fun validate(
    cutovers: List<ModelLineCutover>,
    aliases: Map<String, List<String>>,
    hiddenModelLines: Set<String> = emptySet(),
    sourceModelLines: Set<String> = emptySet(),
  ) {
    require(aliases.values.all { it.isNotEmpty() }) {
      "modelLineMap entries must contain at least one model line"
    }
    val references =
      aliases.keys +
        aliases.values.flatten() +
        hiddenModelLines +
        sourceModelLines +
        cutovers.flatMap {
          listOf(it.externalModelLine, it.historicalModelLine, it.replacementModelLine)
        }
    for (name in references) {
      val key =
        requireNotNull(ModelLineKey.fromName(name)) { "Invalid ModelLine resource name: $name" }
      val ids = listOf(key.modelProviderId, key.modelSuiteId, key.modelLineId)
      require(ids.all { it.isNotEmpty() }) { "Invalid ModelLine resource name: $name" }
      for (id in ids) {
        // Opaque local resource IDs remain supported. Numeric Kingdom API IDs must
        // use their canonical spelling so string ownership matches persisted identity.
        val canonical = runCatching { ApiId(id).externalId.apiId.value }.getOrNull() ?: continue
        require(id == canonical) { "ModelLine references must use canonical API IDs: $name" }
      }
    }
    ModelLineCutoverValidator.validate(cutovers, aliases.values.flatten().toSet())
    val visible = cutovers.mapTo(mutableSetOf()) { it.externalModelLine }
    require(hiddenModelLines.intersect(visible).isEmpty()) {
      "hidden_model_lines must not contain a cutover-visible model line"
    }
    val duplicateTargets =
      aliases.values.flatten().groupingBy { it }.eachCount().filterValues { it > 1 }.keys
    require(duplicateTargets.isEmpty()) {
      "Duplicate external model-line alias targets: $duplicateTargets"
    }
    val contributors =
      cutovers.flatMapTo(mutableSetOf()) { listOf(it.historicalModelLine, it.replacementModelLine) }
    val forbidden = aliases.values.flatten().toSet().intersect(contributors + hiddenModelLines)
    require(forbidden.isEmpty()) {
      "Alias targets must not expose internal contributor or hidden model lines: $forbidden"
    }
  }
}
