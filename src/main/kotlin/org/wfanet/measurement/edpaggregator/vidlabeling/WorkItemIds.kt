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

package org.wfanet.measurement.edpaggregator.vidlabeling

import java.util.UUID

/**
 * Single source of truth for the Secure Computation `WorkItem` resource IDs used across the VID
 * labeling pipeline.
 *
 * A `WorkItem` ID is a *resource ID* (the primary key in `workItems/<id>`, addressable via
 * `GetWorkItem`), so it must satisfy RFC 1034 (`<=63` chars, letter-start, `[a-zA-Z0-9-]`, no `_`).
 * The driving resource names embed Kingdom-derived ids that are base64url (can contain `_`) and
 * long, so each WorkItem ID is a bounded, letter-prefixed deterministic UUID of the driving
 * resource's name rather than a raw concatenation. Determinism keeps a re-publish idempotent
 * (`ALREADY_EXISTS`), and every producer plus the [VidLabelingMonitor] recovery path MUST derive
 * IDs through these helpers so the two never drift — a mismatch makes recovery silently no-op.
 *
 * Distinct from the AIP-155 `request_id`s in [RequestIds]: those are client-supplied Create-RPC
 * dedup keys, not addressable resource IDs.
 */
object WorkItemIds {
  /**
   * `WorkItem` ID for a Phase-0 `SubpoolAssigner` shard, keyed by [uploadName], [modelLineName], and
   * [shardIndex] (one WorkItem per (upload, model line, shard)).
   */
  fun forSubpoolAssigner(uploadName: String, modelLineName: String, shardIndex: Int): String =
    "sa-" + fromKey("subpoolAssignerWorkItem:$uploadName:$modelLineName:$shardIndex")

  /** `WorkItem` ID for a Phase-1 `VidRankBuilder`, keyed by [rankerJobName]. */
  fun forVidRankBuilder(rankerJobName: String): String =
    "vrb-" + fromKey("vidRankBuilderWorkItem:$rankerJobName")

  /**
   * `WorkItem` ID for a Phase-2 `VidLabeler`, keyed by [vidLabelingJobName].
   *
   * Used by BOTH the memoized ranker fan-out and the non-memoized dispatcher: a `VidLabelingJob` is
   * created by exactly one path and its resource ID is globally unique, so a single scheme covers
   * both and lets the monitor re-derive the ID without knowing which path produced the model line.
   */
  fun forVidLabeler(vidLabelingJobName: String): String =
    "vl-" + fromKey("vidLabelingWorkItem:$vidLabelingJobName")

  private fun fromKey(key: String): String = UUID.nameUUIDFromBytes(key.toByteArray()).toString()
}
