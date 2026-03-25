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

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.firstOrNull
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
 */
class DataAvailabilityMonitor(
  private val storageClient: StorageClient,
  private val edpImpressionPath: String,
  private val activeModelLines: Set<ModelLineKey>,
  private val metrics: DataAvailabilityMonitorMetrics = DataAvailabilityMonitorMetrics(),
) {
  init {
    require(!edpImpressionPath.startsWith("/")) { "edpImpressionPath cannot start with a slash" }
    require(!edpImpressionPath.endsWith("/")) { "edpImpressionPath cannot end with a slash" }
    require(activeModelLines.isNotEmpty()) { "activeModelLines must not be empty" }
  }

  /** Result of monitoring a single model line. */
  data class ModelLineStatus(
    val modelLineKey: ModelLineKey,
    val isStale: Boolean?,
    val latestDate: LocalDate,
    val gapDates: List<LocalDate>?,
    val staleDays: Int?,
    val zeroImpressionDates: List<LocalDate>?,
    val datesWithoutDoneBlob: List<LocalDate>?,
  )

  /** Result of a full monitoring check across all active model lines. */
  data class MonitorResult(val statuses: List<ModelLineStatus>)

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
        val dateInfo = getDateInfoForModelLine(modelLineKey)
        buildFullStatus(modelLineKey, dateInfo, today, maxStaleDays)
      }

    statuses.forEach { recordMetrics(it) }

    return MonitorResult(statuses = statuses)
  }

  /**
   * Checks all active model lines for date gaps only.
   *
   * @return A [MonitorResult] containing the status of each active model line.
   */
  suspend fun checkGaps(): MonitorResult {
    val statuses =
      activeModelLines.map { modelLineKey ->
        val dateInfo = getDateInfoForModelLine(modelLineKey)
        buildGapStatus(modelLineKey, dateInfo)
      }

    statuses.forEach { recordMetrics(it) }

    return MonitorResult(statuses = statuses)
  }

  private fun buildFullStatus(
    modelLineKey: ModelLineKey,
    dateInfo: DateInfo,
    today: LocalDate,
    maxStaleDays: Int,
  ): ModelLineStatus {
    val modelLineName = modelLineKey.toName()
    val uploadedDates = dateInfo.datesWithDoneBlob
    require(uploadedDates.isNotEmpty()) {
      "No uploaded dates found for model line: $modelLineName. Check configuration."
    }

    val sortedDates = uploadedDates.sorted()
    val latestDate = sortedDates.last()
    val staleDays = (today.toEpochDay() - latestDate.toEpochDay()).toInt()
    val isStale = staleDays > maxStaleDays
    val gapDates = findGaps(sortedDates)

    logger.log(
      Level.INFO,
      "Model line $modelLineName metrics: staleDays=$staleDays, gaps=${gapDates.size}, " +
        "zeroImpressionDates=${dateInfo.zeroImpressionDates.size}, " +
        "datesWithoutDoneBlob=${dateInfo.datesWithoutDoneBlob.size}",
    )
    if (isStale) {
      logger.log(
        Level.SEVERE,
        "Model line $modelLineName is stale: latest upload is $latestDate ($staleDays days ago)",
      )
    }
    if (gapDates.isNotEmpty()) {
      logger.log(Level.SEVERE, "Model line $modelLineName has gaps: gap dates $gapDates")
    }
    if (dateInfo.zeroImpressionDates.isNotEmpty()) {
      logger.log(
        Level.SEVERE,
        "Model line $modelLineName has zero impression dates (done blob but no data): " +
          "${dateInfo.zeroImpressionDates}",
      )
    }
    if (dateInfo.datesWithoutDoneBlob.isNotEmpty()) {
      logger.log(
        Level.SEVERE,
        "Model line $modelLineName has dates without done blob: ${dateInfo.datesWithoutDoneBlob}",
      )
    }

    return ModelLineStatus(
      modelLineKey = modelLineKey,
      isStale = isStale,
      latestDate = latestDate,
      gapDates = gapDates,
      staleDays = staleDays,
      zeroImpressionDates = dateInfo.zeroImpressionDates,
      datesWithoutDoneBlob = dateInfo.datesWithoutDoneBlob,
    )
  }

  private fun buildGapStatus(modelLineKey: ModelLineKey, dateInfo: DateInfo): ModelLineStatus {
    val modelLineName = modelLineKey.toName()
    val uploadedDates = dateInfo.datesWithDoneBlob
    require(uploadedDates.isNotEmpty()) {
      "No uploaded dates found for model line: $modelLineName. Check configuration."
    }

    val sortedDates = uploadedDates.sorted()
    val gapDates = findGaps(sortedDates)

    logger.log(
      Level.INFO,
      "Model line $modelLineName metrics: gaps=${gapDates.size}, " +
        "zeroImpressionDates=${dateInfo.zeroImpressionDates.size}, " +
        "datesWithoutDoneBlob=${dateInfo.datesWithoutDoneBlob.size}",
    )
    if (gapDates.isNotEmpty()) {
      logger.log(Level.SEVERE, "Model line $modelLineName has gaps: gap dates $gapDates")
    }
    if (dateInfo.zeroImpressionDates.isNotEmpty()) {
      logger.log(
        Level.SEVERE,
        "Model line $modelLineName has zero impression dates (done blob but no data): " +
          "${dateInfo.zeroImpressionDates}",
      )
    }
    if (dateInfo.datesWithoutDoneBlob.isNotEmpty()) {
      logger.log(
        Level.SEVERE,
        "Model line $modelLineName has dates without done blob: ${dateInfo.datesWithoutDoneBlob}",
      )
    }

    return ModelLineStatus(
      modelLineKey = modelLineKey,
      isStale = null,
      latestDate = sortedDates.last(),
      gapDates = gapDates,
      staleDays = null,
      zeroImpressionDates = dateInfo.zeroImpressionDates,
      datesWithoutDoneBlob = dateInfo.datesWithoutDoneBlob,
    )
  }

  /** Info about uploaded dates for a model line. */
  private data class DateInfo(
    val datesWithDoneBlob: Set<LocalDate>,
    val zeroImpressionDates: List<LocalDate>,
    val datesWithoutDoneBlob: List<LocalDate>,
  )

  private suspend fun getDateInfoForModelLine(modelLineKey: ModelLineKey): DateInfo {
    val modelLineId = modelLineKey.modelLineId
    val prefix = if (edpImpressionPath.isEmpty()) "model-line/$modelLineId/" else "$edpImpressionPath/model-line/$modelLineId/"
    return getDateInfo(prefix)
  }

  /**
   * Discovers date folders under the given prefix using delimiter-based listing, then checks each
   * date folder for a "done" blob and at least one data file.
   *
   * This avoids enumerating all blobs in every date folder, which can be slow when folders contain
   * thousands of files. Instead, it:
   * 1. Lists date-level prefixes using [StorageClient.listBlobKeys] with delimiter "/".
   * 2. For each date, checks for the "done" blob via [StorageClient.getBlob].
   * 3. For dates with a done blob, checks for at least one data file via [StorageClient.listBlobs].
   *
   * Expected path format: `{prefix}{date}/done` and `{prefix}{date}/other_files`
   */
  private suspend fun getDateInfo(prefix: String): DateInfo {
    val datesWithDone = mutableSetOf<LocalDate>()
    val zeroImpressionDatesList = mutableListOf<LocalDate>()
    val datesWithoutDoneBlobList = mutableListOf<LocalDate>()

    val datePrefixes = storageClient.listBlobKeys(prefix, "/").toList()

    for (datePrefix in datePrefixes) {
      val dateString = datePrefix.removePrefix(prefix).trimEnd('/')
      val date = LocalDate.parse(dateString)

      val doneBlob = storageClient.getBlob("${prefix}$dateString/done")
      if (doneBlob != null) {
        datesWithDone.add(date)

        // Check if there is at least one non-done file
        val hasData = storageClient.listBlobs("${prefix}$dateString/")
          .firstOrNull { !it.blobKey.endsWith("/done") } != null
        if (!hasData) {
          zeroImpressionDatesList.add(date)
        }
      } else {
        datesWithoutDoneBlobList.add(date)
      }
    }

    return DateInfo(
      datesWithDoneBlob = datesWithDone,
      zeroImpressionDates = zeroImpressionDatesList.sorted(),
      datesWithoutDoneBlob = datesWithoutDoneBlobList.sorted(),
    )
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

  private fun recordMetrics(status: ModelLineStatus) {
    val modelLineName = status.modelLineKey.toName()
    val attrs = Attributes.of(
      MODEL_LINE_ATTR,
      modelLineName,
      EDP_IMPRESSION_PATH_ATTR,
      edpImpressionPath,
    )

    if (status.staleDays != null) {
      metrics.staleDaysGauge.set(status.staleDays.toLong(), attrs)
    }
    if (!status.gapDates.isNullOrEmpty()) {
      metrics.gapCounter.add(status.gapDates.size.toLong(), attrs)
    }
    if (!status.zeroImpressionDates.isNullOrEmpty()) {
      metrics.zeroImpressionDatesCounter.add(status.zeroImpressionDates.size.toLong(), attrs)
    }
    if (!status.datesWithoutDoneBlob.isNullOrEmpty()) {
      metrics.datesWithoutDoneBlobCounter.add(status.datesWithoutDoneBlob.size.toLong(), attrs)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    const val DEFAULT_MAX_STALE_DAYS = 3

    val MODEL_LINE_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_monitor.model_line")
    val EDP_IMPRESSION_PATH_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_monitor.edp_impression_path")
  }
}
