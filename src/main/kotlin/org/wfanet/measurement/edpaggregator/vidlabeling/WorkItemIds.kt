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

/**
 * Single source of truth for the Secure Computation `WorkItem` IDs used across the VID labeling
 * pipeline.
 *
 * Every producer of a `WorkItem` and every consumer that must re-derive its ID (the
 * [VidLabelingMonitor] phase-recovery path) MUST go through these helpers so the two never drift. A
 * mismatch makes recovery silently no-op: the monitor `GetWorkItem`s a name that was never created,
 * logs, and gives up. Each ID is a deterministic function of the driving resource's name, so a
 * re-publish is idempotent (`ALREADY_EXISTS`).
 */
object WorkItemIds {
  /**
   * `WorkItem` ID for a Phase-0 `SubpoolAssigner` shard, keyed by [uploadId], [modelLineName], and
   * [shardIndex] (one WorkItem per (upload, model line, shard)).
   */
  fun forSubpoolAssigner(uploadId: String, modelLineName: String, shardIndex: Int): String =
    "subpool-assigner-$uploadId-${modelLineName.substringAfterLast('/')}-shard-$shardIndex"

  /** `WorkItem` ID for a Phase-1 `VidRankBuilder`, keyed by [rankerJobName]. */
  fun forVidRankBuilder(rankerJobName: String): String =
    "vid-rank-builder-${rankerJobName.substringAfterLast('/')}"

  /**
   * `WorkItem` ID for a Phase-2 `VidLabeler`, keyed by [vidLabelingJobName].
   *
   * Used by BOTH the memoized ranker fan-out (`VidRankBuilder`) and the non-memoized dispatcher
   * ([VidLabelingDispatchSequencer]): a `VidLabelingJob` is created by exactly one path and its
   * resource ID is globally unique, so a single scheme covers both paths and lets the monitor
   * re-derive the ID without knowing which path produced the model line.
   */
  fun forVidLabeler(vidLabelingJobName: String): String =
    "vid-labeler-${vidLabelingJobName.substringAfterLast('/')}"
}
