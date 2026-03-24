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

package org.wfanet.measurement.edpaggregator.dataavailability

import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.storage.StorageClient

/**
 * Monitors impression data availability for staleness and gaps.
 *
 * This class checks each active model line's GCS folder structure for:
 * - **Staleness**: The most recent uploaded date is more than [maxStaleDays] days behind today.
 * - **Gaps**: A date is missing between the earliest and latest uploaded dates.
 *
 * The folder structure is expected to be:
 * ```
 * {edpImpressionPath}/model-line/{modelLineId}/{date}/done
 * ```
 *
 * @property storageClient Client for accessing Cloud Storage blobs.
 * @property edpImpressionPath Base path for EDP impressions (e.g.,
 *   "edp/meta/vid-labeled-impressions").
 * @property activeModelLines Set of model line IDs that are expected to upload daily.
 * @property maxStaleDays Maximum number of days a model line can go without an upload before
 *   alerting.
 * @property clock Provides the current date for staleness checks.
 */
class DataAvailabilityMonitor(
  private val storageClient: StorageClient,
  private val edpImpressionPath: String,
  private val activeModelLines: Set<String>,
  private val maxStaleDays: Int = DEFAULT_MAX_STALE_DAYS,
  private val clock: () -> LocalDate = { LocalDate.now(ZoneOffset.UTC) },
) {
  init {
    require(activeModelLines.isNotEmpty()) { "activeModelLines must not be empty" }
    require(maxStaleDays > 0) { "maxStaleDays must be greater than zero" }
  }

  /** Result of monitoring a single model line. */
  data class ModelLineStatus(
    val modelLineId: String,
    val isStale: Boolean,
    val latestDate: LocalDate?,
    val missingDates: List<LocalDate>,
    val staleDays: Int,
  )

  /** Result of a full monitoring check across all active model lines. */
  data class MonitorResult(val statuses: List<ModelLineStatus>, val hasIssues: Boolean)

  /**
   * Checks all active model lines for staleness and gaps.
   *
   * @return A [MonitorResult] containing the status of each active model line.
   */
  suspend fun check(): MonitorResult {
    val today = clock()
    val statuses = activeModelLines.map { modelLineId -> checkModelLine(modelLineId, today) }

    return MonitorResult(
      statuses = statuses,
      hasIssues = statuses.any { it.isStale || it.missingDates.isNotEmpty() },
    )
  }

  /**
   * Checks a single model line for staleness and gaps by listing date folders in GCS.
   *
   * Date folders are discovered by listing blobs with the model line prefix and extracting unique
   * date components from the blob keys.
   */
  private suspend fun checkModelLine(modelLineId: String, today: LocalDate): ModelLineStatus {
    val prefix = "$edpImpressionPath/model-line/$modelLineId/"
    val uploadedDates = getUploadedDates(prefix)

    if (uploadedDates.isEmpty()) {
      logger.log(Level.WARNING, "No uploaded dates found for model line: $modelLineId")
      return ModelLineStatus(
        modelLineId = modelLineId,
        isStale = true,
        latestDate = null,
        missingDates = emptyList(),
        staleDays = Int.MAX_VALUE,
      )
    }

    val sortedDates = uploadedDates.sorted()
    val latestDate = sortedDates.last()
    val staleDays = (today.toEpochDay() - latestDate.toEpochDay()).toInt()
    val isStale = staleDays > maxStaleDays

    // Find gaps: dates missing between earliest and latest
    val missingDates = findGaps(sortedDates)

    if (isStale) {
      logger.log(
        Level.SEVERE,
        "Model line $modelLineId is stale: latest upload is $latestDate ($staleDays days ago)",
      )
    }
    if (missingDates.isNotEmpty()) {
      logger.log(Level.SEVERE, "Model line $modelLineId has gaps: missing dates $missingDates")
    }

    return ModelLineStatus(
      modelLineId = modelLineId,
      isStale = isStale,
      latestDate = latestDate,
      missingDates = missingDates,
      staleDays = staleDays,
    )
  }

  /**
   * Lists blobs under the given prefix and extracts unique dates from paths containing a "done"
   * blob.
   *
   * Expected path format: `{prefix}{date}/done`
   */
  private suspend fun getUploadedDates(prefix: String): Set<LocalDate> {
    val dates = mutableSetOf<LocalDate>()
    storageClient
      .listBlobs(prefix)
      .toList()
      .filter { blob -> blob.blobKey.endsWith("/done") || blob.blobKey.endsWith("/done.txt") }
      .forEach { blob ->
        val relativePath = blob.blobKey.removePrefix(prefix)
        val dateString = relativePath.substringBefore("/")
        try {
          dates.add(LocalDate.parse(dateString))
        } catch (e: Exception) {
          logger.log(Level.FINE, "Skipping non-date folder: $dateString")
        }
      }
    return dates
  }

  /** Finds dates that are missing in the sequence between the first and last date. */
  private fun findGaps(sortedDates: List<LocalDate>): List<LocalDate> {
    if (sortedDates.size <= 1) return emptyList()

    val dateSet = sortedDates.toSet()
    val missing = mutableListOf<LocalDate>()
    var current = sortedDates.first().plusDays(1)
    val last = sortedDates.last()

    while (current.isBefore(last)) {
      if (current !in dateSet) {
        missing.add(current)
      }
      current = current.plusDays(1)
    }
    return missing
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    const val DEFAULT_MAX_STALE_DAYS = 3
  }
}
