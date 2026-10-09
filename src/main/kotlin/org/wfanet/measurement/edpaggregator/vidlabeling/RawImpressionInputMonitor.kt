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

import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.StorageClient

/** Reconciles one configured raw-impression root directly from object metadata. */
class RawImpressionInputMonitor(
  private val storageClient: StorageClient,
  storageRootUri: String,
  blobPrefix: String,
  private val quietPeriod: Duration,
  excludedBlobPrefixes: Set<String> = emptySet(),
  private val clock: Clock = Clock.systemUTC(),
) {
  private val storageRootUri = storageRootUri.trimEnd('/')
  private val blobPrefix = blobPrefix.trim('/')
  private val listingPrefix = if (this.blobPrefix.isEmpty()) "" else "${this.blobPrefix}/"
  private val excludedBlobPrefixes =
    excludedBlobPrefixes
      .mapTo(mutableSetOf()) { it.trim('/') }
      .filterTo(mutableSetOf()) { it.isNotEmpty() }

  data class DoneObjectIdentity(val blobUri: String, val generation: Long)

  enum class FindingType(val telemetryValue: String) {
    MISSING_DONE("missing_done"),
    DONE_WITHOUT_DATA("done_without_data"),
    UNREGISTERED_DONE("unregistered_done"),
    DATA_AFTER_DONE("data_after_done"),
    AMBIGUOUS_DONE_LAYOUT("ambiguous_done_layout"),
  }

  data class Finding(val type: FindingType, val pathHash: String)

  data class Result(
    val missingDoneDirectories: Long,
    val doneWithoutDataDirectories: Long,
    val unregisteredDoneDirectories: Long,
    val dataFilesAfterDone: Long,
    val ambiguousDoneLayouts: Long,
    val missingRegisteredFiles: Long,
    val objectsScanned: Long,
    val findings: List<Finding>,
  )

  /** Takes ownership of [missingRegisteredBlobKeys] and removes keys observed by the scan. */
  suspend fun scan(
    missingRegisteredBlobKeys: MutableSet<String> = mutableSetOf(),
    registeredDoneObjects: Set<DoneObjectIdentity> = emptySet(),
    ignoredEmptyDoneObjects: Set<DoneObjectIdentity> = emptySet(),
    isNoOpDoneObject: suspend (DoneObjectIdentity, Instant) -> Boolean = { _, _ -> false },
  ): Result {
    val directories = mutableMapOf<String, DirectoryObservation>()
    var objectsScanned = 0L

    storageClient.listBlobs(listingPrefix).collect { blob ->
      if (
        excludedBlobPrefixes.any { excludedPrefix ->
          blob.blobKey == excludedPrefix || blob.blobKey.startsWith("$excludedPrefix/")
        }
      ) {
        return@collect
      }
      val relativeKey = blob.blobKey.removePrefix(listingPrefix)
      if (relativeKey == blob.blobKey || relativeKey.isEmpty()) {
        return@collect
      }
      val directory = relativeKey.substringBeforeLast('/', missingDelimiterValue = "")
      val observation = directories.getOrPut(directory) { DirectoryObservation() }
      missingRegisteredBlobKeys.remove(blob.blobKey)
      if (relativeKey.substringAfterLast('/') == DONE_FILE_NAME) {
        observation.done =
          DoneMarker(
            blobKey = blob.blobKey,
            generation =
              (blob as? ConditionalOperationStorageClient.Blob)?.freshnessToken?.toLongOrNull(),
            createTime = blob.createTime,
          )
      } else if (blob.size > 0L) {
        observation.dataCreateTimes.add(blob.createTime.toEpochMilli())
      }
      objectsScanned++
    }

    val cutoff = clock.instant().minus(quietPeriod)
    val doneMarkers =
      directories
        .mapNotNull { (directory, observation) -> observation.done?.let { directory to it } }
        .toMap()
    val doneMarkersWithData = mutableSetOf<String>()
    val unfinalizedDirectories = mutableSetOf<String>()
    val ambiguousDoneLayouts = mutableSetOf<Pair<String, String>>()
    val lateDataByDoneDirectory = mutableMapOf<String, LateData>()

    for ((directory, observation) in directories) {
      val ancestorMarkers =
        ancestors(directory).mapNotNull { ancestor ->
          doneMarkers[ancestor]?.let { ancestor to it }
        }
      observation.dataCreateTimes.forEach { createTimeMillis ->
        val createTime = Instant.ofEpochMilli(createTimeMillis)
        val finalizingMarkers =
          ancestorMarkers.filter { (_, marker) -> marker.createTime >= createTime }
        finalizingMarkers.forEach { (markerDirectory, _) -> doneMarkersWithData += markerDirectory }
        if (finalizingMarkers.size > 1) {
          for (index in 0 until finalizingMarkers.lastIndex) {
            for (otherIndex in index + 1..finalizingMarkers.lastIndex) {
              ambiguousDoneLayouts +=
                finalizingMarkers[index].first to finalizingMarkers[otherIndex].first
            }
          }
        }
        if (finalizingMarkers.isEmpty()) {
          val newestPriorMarker = ancestorMarkers.maxByOrNull { (_, marker) -> marker.createTime }
          if (newestPriorMarker == null) {
            unfinalizedDirectories += directory
          } else {
            val lateData = lateDataByDoneDirectory.getOrPut(newestPriorMarker.first) { LateData() }
            lateData.count++
            if (createTime > lateData.newestCreateTime) {
              lateData.newestCreateTime = createTime
            }
          }
        }
      }
    }

    val missingDoneDirectories =
      collapseNestedDirectories(
        unfinalizedDirectories.filterTo(mutableSetOf()) { directory ->
          isMature(checkNotNull(directories[directory]).newestDataCreateTime, cutoff)
        }
      )

    val doneWithoutDataDirectories = mutableSetOf<String>()
    val unregisteredDoneDirectories = mutableSetOf<String>()
    for ((directory, marker) in doneMarkers) {
      val identity =
        marker.generation?.let { DoneObjectIdentity("$storageRootUri/${marker.blobKey}", it) }
      if (
        directory !in doneMarkersWithData &&
          isMature(marker.createTime, cutoff) &&
          (identity == null || identity !in ignoredEmptyDoneObjects)
      ) {
        doneWithoutDataDirectories += directory
      }
      if (directory in doneMarkersWithData && isMature(marker.createTime, cutoff)) {
        val isUnregistered =
          identity == null ||
            (identity !in registeredDoneObjects && !isNoOpDoneObject(identity, marker.createTime))
        if (isUnregistered) {
          unregisteredDoneDirectories += directory
        }
      }
    }

    val matureLateData =
      lateDataByDoneDirectory.filterValues { isMature(it.newestCreateTime, cutoff) }
    val matureAmbiguousLayouts =
      ambiguousDoneLayouts.filter { (parent, child) ->
        maxOf(
            checkNotNull(doneMarkers[parent]).createTime,
            checkNotNull(doneMarkers[child]).createTime,
          )
          .let { isMature(it, cutoff) }
      }
    val findings = buildList {
      addSamples(FindingType.MISSING_DONE, missingDoneDirectories)
      addSamples(FindingType.DONE_WITHOUT_DATA, doneWithoutDataDirectories)
      addSamples(FindingType.UNREGISTERED_DONE, unregisteredDoneDirectories)
      addSamples(FindingType.DATA_AFTER_DONE, matureLateData.keys)
      addSamples(
        FindingType.AMBIGUOUS_DONE_LAYOUT,
        matureAmbiguousLayouts.map { (parent, child) -> "$parent\u0000$child" },
      )
    }

    return Result(
      missingDoneDirectories = missingDoneDirectories.size.toLong(),
      doneWithoutDataDirectories = doneWithoutDataDirectories.size.toLong(),
      unregisteredDoneDirectories = unregisteredDoneDirectories.size.toLong(),
      dataFilesAfterDone = matureLateData.values.sumOf { it.count },
      ambiguousDoneLayouts = matureAmbiguousLayouts.size.toLong(),
      missingRegisteredFiles = missingRegisteredBlobKeys.size.toLong(),
      objectsScanned = objectsScanned,
      findings = findings,
    )
  }

  private fun MutableList<Finding>.addSamples(type: FindingType, directories: Collection<String>) {
    for (directory in directories.take(MAX_FINDING_LOGS_PER_TYPE)) {
      add(Finding(type, VidLabelingTraceLogging.sha256("$blobPrefix/$directory")))
    }
  }

  private fun isMature(time: Instant, cutoff: Instant): Boolean =
    quietPeriod.isZero || time <= cutoff

  private fun collapseNestedDirectories(directories: Set<String>): Set<String> {
    val roots = linkedSetOf<String>()
    for (directory in directories.sortedBy { it.length }) {
      if (roots.none { root -> directory == root || directory.startsWith("$root/") }) {
        roots += directory
      }
    }
    return roots
  }

  private fun ancestors(directory: String): List<String> = buildList {
    add("")
    if (directory.isEmpty()) {
      return@buildList
    }
    var current = ""
    for (segment in directory.split('/')) {
      current = if (current.isEmpty()) segment else "$current/$segment"
      add(current)
    }
  }

  private class DirectoryObservation {
    var done: DoneMarker? = null
    val dataCreateTimes = LongValues()

    val newestDataCreateTime: Instant
      get() = Instant.ofEpochMilli(dataCreateTimes.maxOrMinValue())
  }

  private data class DoneMarker(
    val blobKey: String,
    val generation: Long?,
    val createTime: Instant,
  )

  private class LateData {
    var count: Long = 0L
    var newestCreateTime: Instant = Instant.MIN
  }

  private class LongValues {
    private var values = LongArray(INITIAL_DATA_TIMES_CAPACITY)
    private var size = 0

    fun add(value: Long) {
      if (size == values.size) {
        values = values.copyOf(values.size * 2)
      }
      values[size++] = value
    }

    fun forEach(block: (Long) -> Unit) {
      for (index in 0 until size) {
        block(values[index])
      }
    }

    fun maxOrMinValue(): Long {
      if (size == 0) return Long.MIN_VALUE
      var maximum = values[0]
      for (index in 1 until size) {
        maximum = maxOf(maximum, values[index])
      }
      return maximum
    }
  }

  companion object {
    private const val DONE_FILE_NAME = "done"
    private const val INITIAL_DATA_TIMES_CAPACITY = 16
    private const val MAX_FINDING_LOGS_PER_TYPE = 10
  }
}
