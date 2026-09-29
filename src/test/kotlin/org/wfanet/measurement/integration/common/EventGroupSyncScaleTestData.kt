/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import kotlin.random.Random

/** Selects reproducible, disjoint mutations for the EventGroupSync scale test. */
object EventGroupSyncScaleTestData {
  data class MutationSelection(
    val updatedReferenceIds: Set<String>,
    val deletedReferenceIds: Set<String>,
    val omittedReferenceIds: Set<String>,
    val addedReferenceIds: Set<String>,
  )

  fun selectMutations(
    currentReferenceIds: Collection<String>,
    seed: String,
    mutationCount: Int,
  ): MutationSelection {
    require(currentReferenceIds.size >= mutationCount * 3) {
      "At least ${mutationCount * 3} EventGroups are required"
    }
    require(currentReferenceIds.toSet().size == currentReferenceIds.size) {
      "EventGroup reference IDs must be unique"
    }

    val shuffled = currentReferenceIds.sorted().shuffled(Random(seed.hashCode()))
    val updated = shuffled.take(mutationCount).toSet()
    val deleted = shuffled.drop(mutationCount).take(mutationCount).toSet()
    val omitted = shuffled.drop(mutationCount * 2).take(mutationCount).toSet()
    val added =
      newReferenceIds(
        existingReferenceIds = currentReferenceIds,
        seed = seed,
        label = "added",
        count = mutationCount,
      )

    return MutationSelection(
      updatedReferenceIds = updated,
      deletedReferenceIds = deleted,
      omittedReferenceIds = omitted,
      addedReferenceIds = added,
    )
  }

  fun newReferenceIds(
    existingReferenceIds: Collection<String>,
    seed: String,
    label: String,
    count: Int,
  ): Set<String> {
    val existing = existingReferenceIds.toSet()
    val safeSeed = seed.lowercase().replace(Regex("[^a-z0-9-]"), "-").take(MAX_SEED_LENGTH)
    val result = linkedSetOf<String>()
    var index = 0
    while (result.size < count) {
      val candidate = "campaign-scale-test-$safeSeed-$label-${index.toString().padStart(5, '0')}"
      if (candidate !in existing) {
        result += candidate
      }
      index++
    }
    return result
  }

  private const val MAX_SEED_LENGTH = 24
}
