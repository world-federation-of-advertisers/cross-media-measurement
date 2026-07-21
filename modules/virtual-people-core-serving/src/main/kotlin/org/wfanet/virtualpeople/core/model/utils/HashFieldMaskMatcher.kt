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

package org.wfanet.virtualpeople.core.model.utils

import com.google.common.hash.Hashing
import com.google.protobuf.FieldMask
import com.google.protobuf.util.FieldMaskUtil.merge
import java.nio.charset.StandardCharsets
import org.wfanet.virtualpeople.common.LabelerEvent

/** Selects the index of the hash that matches the hash of the input LabelerEvent. */
class HashFieldMaskMatcher(
  private val hashes: Map<ULong, Int>,
  private val hashFieldMask: FieldMask
) {

  /** Returns the index of the matching hash value in [hashes]. Returns -1 if no matching. */
  fun getMatch(event: LabelerEvent): Int {
    return hashes.getOrDefault(hashLabelerEvent(event, hashFieldMask), -1)
  }

  companion object {
    /** Get the hash of [event], only including the fields in [hashFieldMask]. */
    private fun hashLabelerEvent(event: LabelerEvent, hashFieldMask: FieldMask): ULong {
      val result = LabelerEvent.newBuilder()
      merge(hashFieldMask, event, result)
      return Hashing.farmHashFingerprint64()
        .hashString(result.build().toString(), StandardCharsets.UTF_8)
        .asLong()
        .toULong()
    }

    /**
     * build a [HashFieldMaskMatcher] from a list of [LabelerEvent] and a [FieldMask].
     *
     * Returns error status if any of the following happens:
     * 1. [events] is empty.
     * 2. Any entry in [events] is null.
     * 3. [hashFieldMask].paths is empty.
     * 4. Multiple entries in [events] have the same hash value.
     */
    fun build(events: List<LabelerEvent>, hashFieldMask: FieldMask): HashFieldMaskMatcher {
      if (events.isEmpty()) {
        error("The events is empty when building HashFieldMaskMatcher.")
      }
      if (hashFieldMask.pathsCount == 0) {
        error("The hashFieldMask is empty when building HashFieldMaskMatcher.")
      }
      var index = 0
      val hashes: MutableMap<ULong, Int> = mutableMapOf()
      events.forEach {
        val hash = hashLabelerEvent(it, hashFieldMask)
        if (hashes.containsKey(hash)) {
          error("Multiple events have the same hash when applying hash field mask.")
        } else {
          hashes[hash] = index
        }
        ++index
      }
      return HashFieldMaskMatcher(hashes, hashFieldMask)
    }
  }
}
