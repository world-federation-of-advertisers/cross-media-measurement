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

import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
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
 */
class DataAvailabilityMonitor(
  private val storageClient: StorageClient,
  private val edpImpressionPath: String,
  private val activeModelLines: Set<ModelLineKey>,
) {
  init {
    require(activeModelLines.isNotEmpty()) { "activeModelLines must not be empty" }
  }

  /** Result of monitoring a single model line. */
  data class ModelLineStatus(
    val modelLineId: String,
    val isStale: Boolean?,
    val latestDate: LocalDate,
    val missingDates: List<LocalDate>?,
    val staleDays: Int?,
    val emptyDateFolders: List<LocalDate>?,
  )

  /** Result of a full monitoring check across all active model lines. */
  data class MonitorResult(val statuses: List<ModelLineStatus>, val hasIssues: Boolean)

  /**
   * Checks all active model lines for staleness and gaps.
   *
   * @param maxStaleDays Maximum number of days a model line can go without an upload before
   *   alerting.
   * @param clock Provides the current date for staleness checks.
   * @return A [MonitorResult] containing the status of each active model line.
   */
  suspend fun checkFullStatus(maxStaleDays: Int, clock: () -> LocalDate): MonitorResult {
    require(maxStaleDays > 0) { "maxStaleDays must be greater than zero" }
    val today = clock()
    val statuses =
      activeModelLines.map { modelLineKey ->
        val uploadedDates = getUploadedDatesForModelLine(modelLineKey.modelLineId)
        buildFullStatus(modelLineKey, uploadedDates, today, maxStaleDays)
      }

    return MonitorResult(
      statuses = statuses,
      hasIssues =
        statuses.any {
          it.isStale == true || !it.missingDates.isNullOrEmpty() || !it.emptyDateFolders.isNullOrEmpty()
        },
    )
  }

  /**
   * Checks all active model lines for date gaps only.
   *
   * @return A [MonitorResult] where [ModelLineStatus.hasIssues] reflects only gap issues.
   */
  suspend fun checkGaps(): MonitorResult {
    val statuses =
      activeModelLines.map { modelLineKey ->
        val dateInfo = getDateInfoForModelLine(modelLineKey.modelLineId)
        buildGapStatus(modelLineKey, dateInfo)
      }

    return MonitorResult(
      statuses = statuses,
      hasIssues = statuses.any { !it.missingDates.isNullOrEmpty() || !it.emptyDateFolders.isNullOrEmpty() },
    )
  }

  private fun buildFullStatus(
    modelLineKey: ModelLineKey,
    uploadedDates: Set<LocalDate>,
    today: LocalDate,
    maxStaleDays: Int,
  ): ModelLineStatus {
    val modelLineId = modelLineKey.toName()
    require(uploadedDates.isNotEmpty()) {
      "No uploaded dates found for model line: $modelLineId. Check configuration."
    }

    val sortedDates = uploadedDates.sorted()
    val latestDate = sortedDates.last()
    val staleDays = (today.toEpochDay() - latestDate.toEpochDay()).toInt()
    val isStale = staleDays > maxStaleDays
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
      emptyDateFolders = emptyList(),
    )
  }

  private fun buildGapStatus(modelLineKey: ModelLineKey, dateInfo: DateInfo): ModelLineStatus {
    val modelLineId = modelLineKey.toName()
    val uploadedDates = dateInfo.datesWithDoneBlob
    require(uploadedDates.isNotEmpty()) {
      "No uploaded dates found for model line: $modelLineId. Check configuration."
    }

    val sortedDates = uploadedDates.sorted()
    val missingDates = findGaps(sortedDates)

    if (missingDates.isNotEmpty()) {
      logger.log(Level.SEVERE, "Model line $modelLineId has gaps: missing dates $missingDates")
    }

    return ModelLineStatus(
      modelLineId = modelLineId,
      isStale = null,
      latestDate = sortedDates.last(),
      missingDates = missingDates,
      staleDays = null,
      emptyDateFolders = dateInfo.emptyDateFolders,
    )
  }

  /** Info about uploaded dates for a model line. */
  private data class DateInfo(
    val datesWithDoneBlob: Set<LocalDate>,
    val emptyDateFolders: List<LocalDate>,
  )

  private suspend fun getDateInfoForModelLine(modelLineId: String): DateInfo {
    val prefix = if (edpImpressionPath.isEmpty()) "model-line/$modelLineId/" else "$edpImpressionPath/model-line/$modelLineId/"
    return getDateInfo(prefix)
  }

  private suspend fun getUploadedDatesForModelLine(modelLineId: String): Set<LocalDate> {
    return getDateInfoForModelLine(modelLineId).datesWithDoneBlob
  }

  /**
   * Lists blobs under the given prefix, extracts dates from paths containing a "done" blob, and
   * identifies date folders that have a done blob but no other files (empty folders).
   *
   * Expected path format: `{prefix}{date}/done` and `{prefix}{date}/other_files`
   */
  private suspend fun getDateInfo(prefix: String): DateInfo {
    val datesWithDone = mutableSetOf<LocalDate>()
    val datesWithData = mutableSetOf<LocalDate>()

    storageClient.listBlobs(prefix).collect { blob ->
      val relativePath = blob.blobKey.removePrefix(prefix)
      val dateString = relativePath.substringBefore("/")
      val date = LocalDate.parse(dateString)
      if (blob.blobKey.endsWith("/done")) {
        datesWithDone.add(date)
      } else {
        datesWithData.add(date)
      }
    }

    val emptyFolders = (datesWithDone - datesWithData).sorted()
    for (date in emptyFolders) {
      logger.log(Level.SEVERE, "Date folder $date has a done blob but no data files")
    }

    return DateInfo(datesWithDoneBlob = datesWithDone, emptyDateFolders = emptyFolders)
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
