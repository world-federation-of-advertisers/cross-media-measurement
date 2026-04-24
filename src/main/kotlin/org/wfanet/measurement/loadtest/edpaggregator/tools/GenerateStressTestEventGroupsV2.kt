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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.LocalDate
import java.time.Year
import java.time.ZoneId
import java.util.logging.Logger
import kotlin.random.Random
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/**
 * Generates a large batch of EventGroup protos for stress-testing EventGroupSync.
 *
 * Differences from [GenerateStressTestEventGroups]:
 *  - Each EventGroup gets its own randomized [dataAvailabilityInterval] inside a
 *    configurable year (default 2022) so runs can target unused date ranges.
 *  - Defaults to 50,000 campaigns and supports 100,000+.
 *  - Blob key is configurable so successive runs do not overwrite each other.
 *  - Reference IDs are zero-padded to fit campaign counts up to 1,000,000.
 *
 * Output blob: gs://<storageBucket>/<edpName>/event-groups/<blobKey>
 * Writing to that prefix triggers the dev `data-watcher` -> `event-group-sync`
 * Cloud Function via Eventarc.
 */
@Command(
  name = "generate-stress-test-event-groups-v2",
  description = [
    "Generates a large batch of EventGroup protos with randomized per-group date " +
      "ranges and uploads them as a MesosRecordIO blob to GCS for EventGroupSync " +
      "stress testing."
  ],
)
class GenerateStressTestEventGroupsV2 : Runnable {

  @Option(
    names = ["--storage-bucket"],
    description = ["GCS bucket for event group storage."],
    required = true,
  )
  lateinit var storageBucket: String
    private set

  @Option(
    names = ["--edp-name"],
    description = ["EDP directory name in the bucket (e.g. edp7)."],
    required = false,
    defaultValue = "edp7",
  )
  lateinit var edpName: String
    private set

  @Option(
    names = ["--measurement-consumer"],
    description = ["MeasurementConsumer resource name."],
    required = true,
  )
  lateinit var measurementConsumer: String
    private set

  @Option(
    names = ["--campaign-count"],
    description = ["Number of synthetic event groups to generate."],
    required = false,
    defaultValue = "50000",
  )
  var campaignCount: Int = 50_000
    private set

  @Option(
    names = ["--campaign-prefix"],
    description = ["Prefix for event group reference IDs."],
    required = false,
    defaultValue = "stress-v2-campaign",
  )
  lateinit var campaignPrefix: String
    private set

  @Option(
    names = ["--year"],
    description = [
      "Year (UTC) in which to place each event group's random data-availability " +
        "interval. Defaults to 2022."
    ],
    required = false,
    defaultValue = "2022",
  )
  var year: Int = 2022
    private set

  @Option(
    names = ["--min-duration-days"],
    description = ["Minimum data-availability interval duration in days (inclusive)."],
    required = false,
    defaultValue = "1",
  )
  var minDurationDays: Int = 1
    private set

  @Option(
    names = ["--max-duration-days"],
    description = ["Maximum data-availability interval duration in days (inclusive)."],
    required = false,
    defaultValue = "30",
  )
  var maxDurationDays: Int = 30
    private set

  @Option(
    names = ["--seed"],
    description = ["Seed for reproducible random date ranges."],
    required = false,
    defaultValue = "42",
  )
  var seed: Long = 42L
    private set

  @Option(
    names = ["--blob-key"],
    description = [
      "Object key (relative to the bucket) for the generated event-groups blob. " +
        "Must end in .binpb. Defaults to " +
        "<edp-name>/event-groups/<edp-name>-stress-test-v2-event-groups.binpb."
    ],
    required = false,
  )
  var blobKey: String? = null
    private set

  override fun run() {
    require(campaignCount in 1..1_000_000) { "--campaign-count must be in 1..1,000,000" }
    require(minDurationDays in 1..365) { "--min-duration-days must be in 1..365" }
    require(maxDurationDays in minDurationDays..365) {
      "--max-duration-days must be >= --min-duration-days and <= 365"
    }

    val zoneId = ZoneId.of("UTC")
    val yearLength = Year.of(year).length()
    val firstDay = LocalDate.of(year, 1, 1)
    val rng = Random(seed)
    val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")

    // Use a width that fits the campaign count so reference IDs sort and read cleanly.
    val idWidth = campaignCount.toString().length

    val eventGroups = (1..campaignCount).map { i ->
      val refId = "$campaignPrefix-${i.toString().padStart(idWidth, '0')}"

      // Random duration in [minDurationDays, maxDurationDays].
      val duration = rng.nextInt(minDurationDays, maxDurationDays + 1)
      // Random start so the entire interval stays inside the chosen year.
      val maxStartOffset = yearLength - duration
      val startOffset = if (maxStartOffset <= 0) 0 else rng.nextInt(0, maxStartOffset + 1)
      val start = firstDay.plusDays(startOffset.toLong())
      val endInclusive = start.plusDays((duration - 1).toLong())

      val startInstant = start.atStartOfDay(zoneId).toInstant()
      val endInstant = endInclusive.atTime(23, 59, 59).atZone(zoneId).toInstant()

      eventGroup {
        eventGroupReferenceId = refId
        this.measurementConsumer = this@GenerateStressTestEventGroupsV2.measurementConsumer
        dataAvailabilityInterval = interval {
          this.startTime = timestamp { seconds = startInstant.epochSecond }
          this.endTime = timestamp { seconds = endInstant.epochSecond }
        }
        eventGroupMetadata = eventGroupMetadata {
          adMetadata = adMetadata {
            this.campaignMetadata = campaignMetadata {
              brand = "stress-test-brand-v2"
              campaign = refId
            }
          }
        }
        mediaTypes += MediaType.VIDEO
      }
    }

    val effectiveBlobKey =
      blobKey ?: "$edpName/event-groups/$edpName-stress-test-v2-event-groups.binpb"
    require(effectiveBlobKey.endsWith(".binpb")) { "--blob-key must end with .binpb" }

    val blobUri = "gs://$storageBucket/$effectiveBlobKey"
    logger.info(
      "Writing $campaignCount event groups to $blobUri " +
        "(year=$year, duration=$minDurationDays..$maxDurationDays days, seed=$seed)"
    )

    runBlocking {
      val blobUriParsed = SelectedStorageClient.parseBlobUri(blobUri)
      val storageClient =
        MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = blobUriParsed,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        )
      storageClient.writeBlob(effectiveBlobKey, eventGroups.asFlow().map { it.toByteString() })
    }

    val firstRef = "$campaignPrefix-${1.toString().padStart(idWidth, '0')}"
    val lastRef = "$campaignPrefix-${campaignCount.toString().padStart(idWidth, '0')}"
    logger.info("Successfully wrote $campaignCount event groups.")
    logger.info("Event group reference IDs: $firstRef through $lastRef")
    logger.info("Blob URI: $blobUri")
    logger.info(
      "Note: writing this blob to the watched prefix will trigger EventGroupSync " +
        "via the data-watcher Cloud Function."
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(GenerateStressTestEventGroupsV2::class.java.name)
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateStressTestEventGroupsV2(), args)
