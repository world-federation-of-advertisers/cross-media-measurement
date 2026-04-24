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

import com.google.cloud.storage.StorageOptions
import com.google.protobuf.timestamp
import com.google.type.interval
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
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
 * Generates EventGroup protos for the SWIOTLB stress test and uploads them to GCS.
 *
 * This creates [campaignCount] event groups, each covering the date range
 * [startDate] to [endDate], and writes them as a MesosRecordIO blob to the
 * configured GCS bucket. A DataWatcher "done" blob triggers EventGroupSync
 * downstream.
 */
@Command(
  name = "generate-stress-test-event-groups",
  description = ["Generates EventGroup protos for the SWIOTLB stress test."],
)
class GenerateStressTestEventGroups : Runnable {

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
    required = true,
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
    description = ["Number of synthetic campaigns to generate."],
    required = false,
    defaultValue = "63",
  )
  var campaignCount: Int = 63
    private set

  @Option(
    names = ["--campaign-prefix"],
    description = ["Prefix for event group reference IDs."],
    required = false,
    defaultValue = "swiotlb-stress-campaign",
  )
  lateinit var campaignPrefix: String
    private set

  @Option(
    names = ["--start-date"],
    description = ["Start date in ISO format (yyyy-MM-dd)."],
    required = false,
    defaultValue = "2026-03-01",
  )
  lateinit var startDate: String
    private set

  @Option(
    names = ["--end-date"],
    description = ["End date exclusive in ISO format (yyyy-MM-dd)."],
    required = false,
    defaultValue = "2026-04-06",
  )
  lateinit var endDate: String
    private set

  override fun run() {
    val zoneId = ZoneId.of("UTC")
    val start = LocalDate.parse(startDate)
    val endExclusive = LocalDate.parse(endDate)
    val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")

    val eventGroups = (1..campaignCount).map { i ->
      val refId = "$campaignPrefix-${String.format("%02d", i)}"
      val startInstant = start.atStartOfDay(zoneId).toInstant()
      val endInstant = endExclusive.minusDays(1).atTime(23, 59, 59).atZone(zoneId).toInstant()

      eventGroup {
        eventGroupReferenceId = refId
        this.measurementConsumer = this@GenerateStressTestEventGroups.measurementConsumer
        dataAvailabilityInterval = interval {
          this.startTime = timestamp { seconds = startInstant.epochSecond }
          this.endTime = timestamp { seconds = endInstant.epochSecond }
        }
        eventGroupMetadata = eventGroupMetadata {
          adMetadata = adMetadata {
            this.campaignMetadata = campaignMetadata {
              brand = "stress-test-brand"
              campaign = refId
            }
          }
        }
        mediaTypes += MediaType.VIDEO
      }
    }

    val objectKey = "$edpName/event-groups/$edpName-stress-test-event-groups.binpb"
    val blobUri = "gs://$storageBucket/$objectKey"
    logger.info("Writing ${eventGroups.size} event groups to $blobUri")

    runBlocking {
      val blobUriParsed = SelectedStorageClient.parseBlobUri(blobUri)
      val storageClient = MesosRecordIoStorageClient(
        SelectedStorageClient(
          blobUri = blobUriParsed,
          rootDirectory = null,
          projectId = googleProjectId,
        )
      )
      storageClient.writeBlob(objectKey, eventGroups.asFlow().map { it.toByteString() })
    }

    logger.info("Successfully wrote ${eventGroups.size} event groups.")
    logger.info("Event group reference IDs: $campaignPrefix-01 through $campaignPrefix-${String.format("%02d", campaignCount)}")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Run GenerateSyntheticData for each campaign (63 times)")
    logger.info("  2. Create done blobs for each date in the range")
    logger.info("  3. Wait for EventGroupSync and DataAvailabilitySync")
    logger.info("  4. Create a measurement via Benchmark tool referencing all event groups")
  }

  companion object {
    private val logger: Logger = Logger.getLogger(GenerateStressTestEventGroups::class.java.name)
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateStressTestEventGroups(), args)
