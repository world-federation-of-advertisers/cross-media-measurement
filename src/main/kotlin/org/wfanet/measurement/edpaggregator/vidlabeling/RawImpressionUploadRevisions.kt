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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload

/** Ordering and ancestry of registered raw-impression upload revisions. */
object RawImpressionUploadRevisions {
  /** Latest timestamped revision, or the latest resource creation when no done time is present. */
  fun latest(uploads: List<RawImpressionUpload>): RawImpressionUpload? {
    val timestamped = uploads.filter { it.hasDoneBlobCreateTime() }
    return if (timestamped.isNotEmpty()) {
      timestamped.maxWithOrNull { left, right ->
        val doneTime = Timestamps.compare(left.doneBlobCreateTime, right.doneBlobCreateTime)
        if (doneTime != 0) {
          doneTime
        } else {
          val createTime = Timestamps.compare(left.createTime, right.createTime)
          if (createTime != 0) createTime else left.name.compareTo(right.name)
        }
      }
    } else {
      uploads.maxWithOrNull { left, right -> Timestamps.compare(left.createTime, right.createTime) }
    }
  }

  /** Whether [candidateName]'s replacement chain contains [ancestorName]. */
  fun replaces(
    candidateName: String,
    ancestorName: String,
    revisions: List<RawImpressionUpload>,
  ): Boolean = replaces(candidateName, ancestorName, revisions.associateBy { it.name })

  /** Whether [candidateName]'s replacement chain contains [ancestorName]. */
  fun replaces(
    candidateName: String,
    ancestorName: String,
    revisionsByName: Map<String, RawImpressionUpload>,
  ): Boolean {
    val visited = mutableSetOf<String>()
    var current = revisionsByName[candidateName]?.replacesRawImpressionUpload.orEmpty()
    while (current.isNotEmpty() && visited.add(current)) {
      if (current == ancestorName) return true
      current = revisionsByName[current]?.replacesRawImpressionUpload.orEmpty()
    }
    return false
  }
}
