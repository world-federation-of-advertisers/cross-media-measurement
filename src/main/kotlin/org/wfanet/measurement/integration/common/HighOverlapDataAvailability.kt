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

package org.wfanet.measurement.integration.common

import java.time.LocalDate
import java.util.logging.Logger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.storage.ConditionalOperationStorageClient

/** Publishes completion notifications and verifies high overlap data availability. */
class HighOverlapDataAvailability(
  private val storageClient: ConditionalOperationStorageClient,
  private val modelLineId: String,
  private val publicationTimeout: Duration = 15.minutes,
  private val pollingInterval: Duration = 5.seconds,
) {
  init {
    require(modelLineId.isNotEmpty()) { "modelLineId must not be empty" }
    require(publicationTimeout.isPositive()) { "publicationTimeout must be positive" }
    require(pollingInterval.isPositive()) { "pollingInterval must be positive" }
  }

  /**
   * Writes dates in order within each path, with at most four notifications awaiting publication.
   */
  suspend fun publishDates(datesByImpressionPath: Map<String, Set<LocalDate>>) {
    val permits = Semaphore(MAX_CONCURRENT_DATES)
    coroutineScope {
      datesByImpressionPath
        .map { (path, dates) ->
          async {
            for (date in dates.sorted()) {
              permits.withPermit {
                val key = doneBlobKey(path, date)
                val written = storageClient.writeBlobIfNotFound(key, emptyFlow())
                val token = (written as ConditionalOperationStorageClient.Blob).freshnessToken
                awaitPublication(key, token)
                logger.info("High overlap data availability published for $key")
              }
            }
          }
        }
        .awaitAll()
    }
    requirePublished(datesByImpressionPath)
  }

  /** Rejects fixtures whose completion notifications have not been published to the Kingdom. */
  suspend fun requirePublished(datesByImpressionPath: Map<String, Set<LocalDate>>) {
    for ((path, dates) in datesByImpressionPath) {
      for (date in dates.sorted()) {
        val key = doneBlobKey(path, date)
        val blob = storageClient.getBlob(key)
        check(blob != null && DataAvailabilityBlobs.isDataAvailabilityPublished(blob)) {
          "High overlap data availability is not published for $key. Re-run seeding or replay data availability."
        }
      }
    }
  }

  private suspend fun awaitPublication(key: String, token: String) {
    val published =
      withTimeoutOrNull(publicationTimeout) {
        while (true) {
          val blob =
            checkNotNull(storageClient.getBlob(key)) {
              "Completion blob $key disappeared during seeding"
            }
          check(blob is ConditionalOperationStorageClient.Blob && blob.freshnessToken == token) {
            "Completion blob $key was replaced during seeding"
          }
          if (DataAvailabilityBlobs.isDataAvailabilityPublished(blob)) break
          delay(pollingInterval)
        }
        true
      }
    check(published == true) {
      "Data availability for $key was not published within $publicationTimeout"
    }
  }

  private fun doneBlobKey(path: String, date: LocalDate): String =
    "$path/model-line/$modelLineId/$date/done"

  companion object {
    private val logger = Logger.getLogger(HighOverlapDataAvailability::class.java.name)
    // Leave capacity for other DataAvailabilitySync requests during fixture seeding.
    private const val MAX_CONCURRENT_DATES = 4
  }
}
