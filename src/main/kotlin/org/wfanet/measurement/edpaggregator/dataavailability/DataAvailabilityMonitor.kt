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

import com.google.protobuf.timestamp
import com.google.type.interval
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata as V1AlphaImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter as listImpressionMetadataRequestFilter
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient

/**
 * Monitors impression data availability for staleness, gaps, missing days, late-arriving files, and
 * spuriously deleted metadata entries.
 *
 * This class checks each active model line's GCS folder structure for:
 * - **Staleness**: The most recent uploaded date is more than [maxStaleDays] days behind today.
 * - **Gaps**: A date is missing between the earliest and latest uploaded dates.
 * - **Zero-impression dates**: A date folder has a "done" blob but no data files.
 * - **Missing done blobs**: A date folder exists but has no "done" blob.
 * - **Late-arriving files**: Data files were updated after the "done" blob was written.
 * - **Spurious deletions**: ImpressionMetadata entries marked as deleted whose blobs still exist on
 *   the bucket, indicating the deletion was caused by a GCS overwrite race condition rather than an
 *   intentional deletion.
 *
 * The folder structure is expected to be:
 * ```
 * {edpImpressionPath}/model-line/{modelLineId}/{date}/done
 * ```
 *
 * @property storageClient Client for accessing Cloud Storage blobs.
 * @property edpImpressionPath Base path for EDP impressions (e.g.,
 *   "edp/edp1/vid-labeled-impressions").
 * @property activeModelLines Set of model line keys that are expected to upload daily.
 * @property impressionMetadataStub Optional gRPC stub for the ImpressionMetadata service. Required
 *   only for [checkSpuriousDeletions].
 * @property dataProviderName Optional data provider resource name (e.g.,
 *   "dataProviders/P8xrCkbWKBM"). Required only for [checkSpuriousDeletions].
 */
class DataAvailabilityMonitor(
  private val storageClient: StorageClient,
  private val edpImpressionPath: String,
  private val activeModelLines: Set<ModelLineKey>,
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub? = null,
  private val dataProviderName: String? = null,
) {
  init {
    require(!edpImpressionPath.startsWith("/")) { "edpImpressionPath cannot start with a slash" }
    require(!edpImpressionPath.endsWith("/")) { "edpImpressionPath cannot end with a slash" }
    require(activeModelLines.isNotEmpty()) { "activeModelLines must not be empty" }
  }

  /**
   * Result of monitoring a single model line.
   *
   * @property modelLineKey Identifies the model line that was checked.
   * @property isStale `true` if the model line exceeds [maxStaleDays], `false` if within threshold,
   *   or `null` when staleness was not checked (e.g., gap-only mode or no uploaded dates found).
   * @property latestDate The most recent date with a completed upload ("done" blob). Set to the
   *   current date when no uploads are found (staleness mode) or [LocalDate.MIN] in gap-only mode.
   * @property gapDates Dates missing between the earliest and latest uploaded dates, or `null` when
   *   no uploaded dates were found.
   * @property staleDays Number of days between today and [latestDate], or `null` when staleness was
   *   not checked.
   * @property zeroImpressionDates Dates with a "done" blob but no data files, or `null` when no
   *   uploaded dates were found.
   * @property datesWithoutDoneBlob Date folders that exist but have no "done" blob.
   * @property lateArrivingDates Dates where data files were updated after the "done" blob, or
   *   `null` when no uploaded dates were found.
   * @property healthyDates Dates with a "done" blob, data files, and no late arrivals, or `null`
   *   when no uploaded dates were found.
   * @property spuriousDeletionCount Number of deleted ImpressionMetadata entries whose blob still
   *   exists on the bucket, or `null` when the check was not run.
   * @property legitimateDeletionCount Number of deleted ImpressionMetadata entries whose blob is
   *   genuinely gone from the bucket, or `null` when the check was not run.
   */
  data class ModelLineStatus(
    val modelLineKey: ModelLineKey,
    val isStale: Boolean?,
    val latestDate: LocalDate,
    val gapDates: List<LocalDate>?,
    val staleDays: Int?,
    val zeroImpressionDates: List<LocalDate>?,
    val datesWithoutDoneBlob: List<LocalDate>?,
    val lateArrivingDates: List<LocalDate>?,
    val healthyDates: List<LocalDate>?,
    val spuriousDeletionCount: Int?,
    val legitimateDeletionCount: Int?,
  )

  /**
   * Result of a monitoring check across all active model lines.
   *
   * @property statuses One [ModelLineStatus] per model line in [activeModelLines], in the same
   *   iteration order. The list has a 1:1 correspondence with [activeModelLines].
   */
  data class MonitorResult(val statuses: List<ModelLineStatus>)

  /**
   * Checks all active model lines for staleness, gaps, and data quality issues.
   *
   * @param maxStaleDays Maximum number of days a model line can go without an upload before being
   *   considered stale.
   * @param clock Provides the current date for staleness checks.
   * @param spuriousDeletionLookbackDays If set and [impressionMetadataStub] is available, also
   *   checks for deleted ImpressionMetadata entries whose blobs still exist on the bucket. Only
   *   entries with intervals within the last N days are checked.
   * @return A [MonitorResult] with one [ModelLineStatus] per model line in [activeModelLines], in
   *   the same iteration order.
   */
  suspend fun checkFullStatus(
    maxStaleDays: Int,
    clock: () -> LocalDate,
    spuriousDeletionLookbackDays: Int?,
  ): MonitorResult {
    require(maxStaleDays > 0) { "maxStaleDays must be greater than zero" }
    val today = clock()
    val statuses =
      activeModelLines.map { modelLineKey ->
        val dateInfo = getDateInfoForModelLine(modelLineKey, spuriousDeletionLookbackDays, clock)
        buildFullStatus(modelLineKey, dateInfo, today, maxStaleDays)
      }
    statuses.forEach { recordMetrics(it) }
    return MonitorResult(statuses = statuses)
  }

  /**
   * Checks all active model lines for date gaps and data quality issues only (no staleness).
   *
   * Staleness-related fields ([ModelLineStatus.isStale] and [ModelLineStatus.staleDays]) are `null`
   * in the returned statuses.
   *
   * @return A [MonitorResult] with one [ModelLineStatus] per model line in [activeModelLines], in
   *   the same iteration order.
   */
  suspend fun checkGaps(): MonitorResult {
    val statuses =
      activeModelLines.map { modelLineKey ->
        val dateInfo = getDateInfoForModelLine(modelLineKey, null, null)
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
    if (uploadedDates.isEmpty()) {
      logger.warning("No uploaded dates found for model line: $modelLineName. Check configuration.")
      return ModelLineStatus(
        modelLineKey = modelLineKey,
        isStale = null,
        latestDate = today,
        gapDates = null,
        staleDays = null,
        zeroImpressionDates = null,
        datesWithoutDoneBlob = dateInfo.datesWithoutDoneBlob,
        lateArrivingDates = null,
        healthyDates = null,
        spuriousDeletionCount = dateInfo.spuriousDeletionCount,
        legitimateDeletionCount = dateInfo.legitimateDeletionCount,
      )
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
        Level.WARNING,
        "Model line $modelLineName is stale: latest upload is $latestDate ($staleDays days ago)",
      )
    }

    logDateInfoIssues(modelLineName, gapDates, dateInfo)

    return ModelLineStatus(
      modelLineKey = modelLineKey,
      isStale = isStale,
      latestDate = latestDate,
      gapDates = gapDates,
      staleDays = staleDays,
      zeroImpressionDates = dateInfo.zeroImpressionDates,
      datesWithoutDoneBlob = dateInfo.datesWithoutDoneBlob,
      lateArrivingDates = dateInfo.lateArrivingDates,
      healthyDates = dateInfo.healthyDates,
      spuriousDeletionCount = dateInfo.spuriousDeletionCount,
      legitimateDeletionCount = dateInfo.legitimateDeletionCount,
    )
  }

  private fun buildGapStatus(modelLineKey: ModelLineKey, dateInfo: DateInfo): ModelLineStatus {
    val modelLineName = modelLineKey.toName()
    val uploadedDates = dateInfo.datesWithDoneBlob
    if (uploadedDates.isEmpty()) {
      logger.warning("No uploaded dates found for model line: $modelLineName. Check configuration.")
      return ModelLineStatus(
        modelLineKey = modelLineKey,
        isStale = null,
        latestDate = LocalDate.MIN,
        gapDates = null,
        staleDays = null,
        zeroImpressionDates = null,
        datesWithoutDoneBlob = dateInfo.datesWithoutDoneBlob,
        lateArrivingDates = null,
        healthyDates = null,
        spuriousDeletionCount = dateInfo.spuriousDeletionCount,
        legitimateDeletionCount = dateInfo.legitimateDeletionCount,
      )
    }

    val sortedDates = uploadedDates.sorted()
    val gapDates = findGaps(sortedDates)

    logger.log(
      Level.INFO,
      "Model line $modelLineName metrics: gaps=${gapDates.size}, " +
        "zeroImpressionDates=${dateInfo.zeroImpressionDates.size}, " +
        "datesWithoutDoneBlob=${dateInfo.datesWithoutDoneBlob.size}",
    )

    logDateInfoIssues(modelLineName, gapDates, dateInfo)

    return ModelLineStatus(
      modelLineKey = modelLineKey,
      isStale = null,
      latestDate = sortedDates.last(),
      gapDates = gapDates,
      staleDays = null,
      zeroImpressionDates = dateInfo.zeroImpressionDates,
      datesWithoutDoneBlob = dateInfo.datesWithoutDoneBlob,
      lateArrivingDates = dateInfo.lateArrivingDates,
      healthyDates = dateInfo.healthyDates,
      spuriousDeletionCount = dateInfo.spuriousDeletionCount,
      legitimateDeletionCount = dateInfo.legitimateDeletionCount,
    )
  }

  private fun logDateInfoIssues(
    modelLineName: String,
    gapDates: List<LocalDate>,
    dateInfo: DateInfo,
  ) {
    if (gapDates.isNotEmpty()) {
      logger.log(Level.WARNING, "Model line $modelLineName has gaps: gap dates $gapDates")
    }
    if (dateInfo.zeroImpressionDates.isNotEmpty()) {
      logger.log(
        Level.WARNING,
        "Model line $modelLineName has zero impression dates (done blob but no data): " +
          "${dateInfo.zeroImpressionDates}",
      )
    }
    if (dateInfo.datesWithoutDoneBlob.isNotEmpty()) {
      logger.log(
        Level.WARNING,
        "Model line $modelLineName has dates without done blob: ${dateInfo.datesWithoutDoneBlob}",
      )
    }
    if (dateInfo.lateArrivingDates.isNotEmpty()) {
      logger.log(
        Level.WARNING,
        "Model line $modelLineName has late-arriving data after done blob: " +
          "${dateInfo.lateArrivingDates}",
      )
    }
  }

  private data class DateInfo(
    val datesWithDoneBlob: Set<LocalDate>,
    val zeroImpressionDates: List<LocalDate>,
    val datesWithoutDoneBlob: List<LocalDate>,
    val lateArrivingDates: List<LocalDate>,
    val healthyDates: List<LocalDate>,
    val spuriousDeletionCount: Int?,
    val legitimateDeletionCount: Int?,
  )

  @OptIn(ExperimentalCoroutinesApi::class)
  private suspend fun getDateInfoForModelLine(
    modelLineKey: ModelLineKey,
    spuriousDeletionLookbackDays: Int?,
    clock: (() -> LocalDate)?,
  ): DateInfo {
    val modelLineId = modelLineKey.modelLineId
    val prefix =
      if (edpImpressionPath.isEmpty()) "model-line/$modelLineId/"
      else "$edpImpressionPath/model-line/$modelLineId/"
    val baseInfo = getDateInfo(prefix)

    if (
      spuriousDeletionLookbackDays == null ||
        impressionMetadataStub == null ||
        dataProviderName == null ||
        clock == null
    ) {
      return baseInfo
    }

    require(spuriousDeletionLookbackDays > 0) { "spuriousDeletionLookbackDays must be > 0" }
    require(dataProviderName.isNotBlank()) { "dataProviderName must not be blank" }

    val modelLineName = modelLineKey.toName()
    val today = clock()
    val cutoffDate = today.minusDays(spuriousDeletionLookbackDays.toLong())
    val cutoffEpochSeconds = cutoffDate.atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond()
    val nowEpochSeconds = java.time.Instant.now().epochSecond

    var spuriousCount = 0
    var legitimateCount = 0

    val deletedEntries: Flow<V1AlphaImpressionMetadata> =
      impressionMetadataStub
        .listResources<V1AlphaImpressionMetadata, String, ImpressionMetadataServiceCoroutineStub> {
          pageToken ->
          val response =
            listImpressionMetadata(
              listImpressionMetadataRequest {
                parent = dataProviderName
                pageSize = 100
                showDeleted = true
                this.pageToken = pageToken
                filter = listImpressionMetadataRequestFilter {
                  modelLine = modelLineName
                  intervalOverlaps = interval {
                    startTime = timestamp { seconds = cutoffEpochSeconds }
                    endTime = timestamp { seconds = nowEpochSeconds }
                  }
                }
              }
            )
          ResourceList(response.impressionMetadataList, response.nextPageToken)
        }
        .flattenConcat()

    deletedEntries.collect { entry ->
      val blobKey = SelectedStorageClient.parseBlobUri(entry.blobUri).key
      val blob = storageClient.getBlob(blobKey)
      if (blob != null) {
        spuriousCount++
        if (spuriousCount <= 10) {
          logger.log(
            Level.WARNING,
            "Spurious deletion detected for model line $modelLineName: " +
              "entry ${entry.name} is deleted but blob exists at ${entry.blobUri}",
          )
        }
      } else {
        legitimateCount++
      }
    }

    logger.log(
      Level.INFO,
      "Model line $modelLineName spurious deletion check: " +
        "spurious=$spuriousCount, legitimate=$legitimateCount",
    )
    if (spuriousCount > 0) {
      logger.log(
        Level.SEVERE,
        "ALERT: Model line $modelLineName has $spuriousCount spuriously deleted entries " +
          "(deleted in metadata store but blob still exists on bucket)",
      )
    }

    return baseInfo.copy(
      spuriousDeletionCount = spuriousCount,
      legitimateDeletionCount = legitimateCount,
    )
  }

  private suspend fun getDateInfo(prefix: String): DateInfo {
    val datesWithDone = mutableSetOf<LocalDate>()
    val zeroImpressionDatesList = mutableListOf<LocalDate>()
    val datesWithoutDoneBlobList = mutableListOf<LocalDate>()
    val lateArrivingDatesList = mutableListOf<LocalDate>()
    val healthyDatesList = mutableListOf<LocalDate>()

    val datePrefixes = storageClient.listBlobKeysAndPrefixes(prefix).toList()

    for (datePrefix in datePrefixes) {
      val dateString = datePrefix.removePrefix(prefix).trimEnd('/')
      val date =
        try {
          LocalDate.parse(dateString)
        } catch (e: DateTimeParseException) {
          logger.warning("Skipping non-date folder: $dateString")
          continue
        }

      val doneBlob = storageClient.getBlob("${prefix}$dateString/done")
      if (doneBlob != null) {
        datesWithDone.add(date)

        val hasData =
          storageClient.listBlobs("${prefix}$dateString/").take(2).toList().any {
            !it.blobKey.endsWith("/done") && it.size > 0
          }
        if (!hasData) {
          zeroImpressionDatesList.add(date)
        }

        val hasLateArrivals =
          storageClient
            .listBlobsUpdatedAfter("${prefix}$dateString/", doneBlob.updateTime)
            .take(2)
            .toList()
            .any { !it.blobKey.endsWith("/done") && it.size > 0 }
        if (hasLateArrivals) {
          lateArrivingDatesList.add(date)
        }

        if (hasData && !hasLateArrivals) {
          healthyDatesList.add(date)
        }
      } else {
        datesWithoutDoneBlobList.add(date)
      }
    }

    return DateInfo(
      datesWithDoneBlob = datesWithDone,
      zeroImpressionDates = zeroImpressionDatesList.sorted(),
      datesWithoutDoneBlob = datesWithoutDoneBlobList.sorted(),
      lateArrivingDates = lateArrivingDatesList.sorted(),
      healthyDates = healthyDatesList.sorted(),
      spuriousDeletionCount = null,
      legitimateDeletionCount = null,
    )
  }

  private fun findGaps(sortedDates: List<LocalDate>): List<LocalDate> {
    if (sortedDates.size <= 1) return emptyList()

    val dateSet = sortedDates.toSet()
    return generateSequence(sortedDates.first().plusDays(1)) { it.plusDays(1) }
      .takeWhile { it.isBefore(sortedDates.last()) }
      .filter { it !in dateSet }
      .toList()
  }

  private fun recordMetrics(status: ModelLineStatus) {
    val modelLineName = status.modelLineKey.toName()
    val baseAttrs =
      Attributes.of(MODEL_LINE_ATTR, modelLineName, EDP_IMPRESSION_PATH_ATTR, edpImpressionPath)

    if (status.staleDays != null) {
      DataAvailabilityMonitorMetrics.staleDaysGauge.set(status.staleDays.toLong(), baseAttrs)
    }

    fun addDateCount(count: Int, statusValue: String) {
      if (count > 0) {
        val attrs =
          baseAttrs
            .toBuilder()
            .put(DataAvailabilityMonitorMetrics.DATE_STATUS_ATTR, statusValue)
            .build()
        DataAvailabilityMonitorMetrics.dateStatusCounter.add(count.toLong(), attrs)
      }
    }

    addDateCount(status.gapDates?.size ?: 0, DataAvailabilityMonitorMetrics.STATUS_GAP)
    addDateCount(
      status.zeroImpressionDates?.size ?: 0,
      DataAvailabilityMonitorMetrics.STATUS_ZERO_IMPRESSION,
    )
    addDateCount(
      status.datesWithoutDoneBlob?.size ?: 0,
      DataAvailabilityMonitorMetrics.STATUS_WITHOUT_DONE_BLOB,
    )
    addDateCount(
      status.lateArrivingDates?.size ?: 0,
      DataAvailabilityMonitorMetrics.STATUS_LATE_ARRIVING,
    )
    addDateCount(status.healthyDates?.size ?: 0, DataAvailabilityMonitorMetrics.STATUS_HEALTHY)
    addDateCount(
      status.spuriousDeletionCount ?: 0,
      DataAvailabilityMonitorMetrics.STATUS_SPURIOUS_DELETION,
    )
    addDateCount(
      status.legitimateDeletionCount ?: 0,
      DataAvailabilityMonitorMetrics.STATUS_LEGITIMATE_DELETION,
    )
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
