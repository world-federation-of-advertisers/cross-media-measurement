/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.io.File
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEvent
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionColumns
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionsWriter
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/**
 * BENCHMARK-ONLY (throwaway) generator of PME-encrypted raw-impression Parquet for the memoized VID
 * stress test, uploaded to Cloud Storage. Produces one ~150 MB file for a contiguous **fingerprint
 * key** range `[--k-start, --k-end)` and one event date. Run many instances in parallel (disjoint k
 * ranges, distinct `--blob-key-id`) to make the ~112 files of a 1B-fingerprint day.
 *
 * ## Fingerprint model (why not
 * [org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration])
 * Memoization keys the rank index by `digest(event_id)`, so a fingerprint recurs across days iff
 * its `event_id` string recurs AND it routes to the same subpool. This generator makes the whole
 * row a deterministic function of one integer key `k`, so reusing a k across days reproduces the
 * fingerprint exactly:
 * * `event_id = "fp-$k"` (unique per k; a shared k across days is a guaranteed memoization hit),
 * * the person is `persons[k mod P]` drawn from a **balanced** pool of [personCount] people spread
 *   equally over the 6 `(gender x age)` subpools the reference model routes on, so:
 *     * every day's fingerprints split **evenly across all 6 subpools** (the historical
 *       population-spec generator skewed the 55+ subpools to ~half), and
 *     * a reused k lands in the **same subpool** every day (subpool is a pure function of
 *       gender+age, and the model has exactly one pool per `(gender x age)` leaf).
 *
 * ## Cross-day overlap (driven entirely by the k ranges the orchestration picks)
 * With per-day fingerprint count `N`:
 * * day 1: `k in [0, N)`,
 * * day 2: `k in [N/2, 3N/2)` -> exactly 50% of day 2 was seen on day 1,
 * * day 3: `k in [0, N/2) + [N, 3N/2)` -> 50% first seen on day 1, 50% first seen on day 2.
 *
 * The union is `[0, 3N/2)`, so cumulative distinct fingerprints per subpool are `~1.5N/6` — well
 * under a 1B `ranked_size`, so overflow stays ~0 (which the run is meant to confirm).
 */
@Command(
  name = "generate-benchmark-raw-impressions",
  description = ["Generates one balanced raw-impression Parquet file for a fingerprint-key range."],
)
class GenerateBenchmarkRawImpressions : Runnable {
  @Option(names = ["--output-bucket"], required = true) lateinit var outputBucket: String
  @Option(names = ["--output-prefix"], required = false, defaultValue = "edp/edp7")
  lateinit var outputPrefix: String
  @Option(names = ["--blob-key-id"], required = true) lateinit var blobKeyId: String
  @Option(names = ["--kek-uri"], required = true) lateinit var kekUri: String
  @Option(names = ["--date"], required = true, defaultValue = "2021-03-21")
  lateinit var date: String

  @Option(
    names = ["--k-start"],
    required = true,
    description = ["Inclusive start of this file's fingerprint-key range (event_id = \"fp-<k>\")."],
  )
  var kStart: Long = 0L

  @Option(
    names = ["--k-end"],
    required = true,
    description = ["Exclusive end of this file's fingerprint-key range."],
  )
  var kEnd: Long = 0L

  @Option(
    names = ["--persons"],
    required = false,
    defaultValue = "6000000",
    description = ["Balanced person-pool size P (must be a multiple of 6); person = k mod P."],
  )
  var personCount: Long = 6_000_000L

  override fun run() {
    require(kEnd > kStart) { "--k-end ($kEnd) must be > --k-start ($kStart)" }
    require(personCount > 0 && personCount % 6 == 0L) {
      "--persons ($personCount) must be a positive multiple of 6"
    }
    val targetDate = LocalDate.parse(date)
    // Fixed intra-day timestamp (noon UTC) so every impression falls inside the model line's active
    // window; the footer event_date (written by the writer) is what the pipeline keys the day on.
    val timestamp: Instant = targetDate.atTime(12, 0).toInstant(ZoneOffset.UTC)
    val kmsClient: KmsClient = GcpKmsClient().withDefaultCredentials()
    val storage: Storage = StorageOptions.getDefaultInstance().service
    val localRoot: File = Files.createTempDirectory("bench-raw").toFile()
    val rootPath = Path(localRoot.toURI())

    val writer =
      RawImpressionsWriter(
        blobKeyId = blobKeyId,
        kekUri = kekUri,
        kmsClient = kmsClient,
        storageConfiguration = Configuration(),
        rootPath = rootPath,
        requiredEntityKeyColumns =
          mapOf(RawImpressionColumns.ENTITY_TYPE_PERSON to RawImpressionColumns.PERSON_ID),
      )

    // One impression per k in [kStart, kEnd): person = persons[k mod P], event_id = "fp-<k>".
    val shard =
      EntityKeyedLabeledEventDateShard(
        localDate = targetDate,
        entityKeysWithLabeledEvents =
          (kStart until kEnd).asSequence().map { k ->
            val person = personFor(k, personCount)
            EntityKeysWithLabeledEvents(
              entityKeys =
                listOf(EntityKey(RawImpressionColumns.ENTITY_TYPE_PERSON, "person-${person.vid}")),
              labeledEvents = sequenceOf(LabeledEvent(timestamp, person.vid, person.message)),
            )
          },
      )

    val blobKeys: List<String> = runBlocking {
      writer.writeRawImpressions(
        shards = sequenceOf(shard),
        blobKeyPrefix = outputPrefix,
        eventColumns = { event -> testEventColumns(event) },
        // Position i in the shard is k = kStart + i (events emitted in k order), so the fingerprint
        // is reproducible across days from k alone.
        eventIdFor = { _, _, index -> "fp-${kStart + index}" },
      )
    }
    for (blobKey in blobKeys) {
      val local = File(localRoot, blobKey)
      storage.create(
        BlobInfo.newBuilder(BlobId.of(outputBucket, blobKey)).build(),
        local.readBytes(),
      )
      logger.info(
        "uploaded gs://" + outputBucket + "/" + blobKey + " (" + local.length() + " bytes)"
      )
    }
  }

  /** A synthetic person: a unique [vid] plus the demographics the reference model routes on. */
  private data class BenchPerson(val vid: Long, val message: TestEvent)

  /**
   * Deterministically maps a fingerprint key [k] to one of [personCount] balanced people. The
   * person index `k mod P` is decomposed as `band = index % 6` and `withinBand = index / 6`, so
   * consecutive keys cycle through all 6 `(gender x age)` bands in turn — an exactly even split
   * when the range length is a multiple of 6. The vid only has to be a stable unique person id (the
   * model reads demographics from the gender/age columns, not the population spec), so bands are
   * spaced [BAND_STRIDE] apart to keep vids collision-free.
   */
  private fun personFor(k: Long, personCount: Long): BenchPerson {
    val personIndex = Math.floorMod(k, personCount)
    val band = (personIndex % 6).toInt()
    val withinBand = personIndex / 6
    val vid = band * BAND_STRIDE + withinBand + 1
    val gender = if (band < 3) Person.Gender.MALE else Person.Gender.FEMALE
    val ageGroup = AGE_GROUPS[band % 3]
    val message =
      TestEvent.newBuilder()
        .apply {
          personBuilder.gender = gender
          personBuilder.ageGroup = ageGroup
        }
        .build()
    return BenchPerson(vid, message)
  }

  companion object {
    private val logger = Logger.getLogger(GenerateBenchmarkRawImpressions::class.java.name)

    // 6 subpools = gender x age. Bands 0-2 = MALE {18-34, 35-54, 55+}, bands 3-5 = FEMALE (same).
    private val AGE_GROUPS =
      listOf(
        Person.AgeGroup.YEARS_18_TO_34,
        Person.AgeGroup.YEARS_35_TO_54,
        Person.AgeGroup.YEARS_55_PLUS,
      )

    // Vid namespace width reserved per band; > any realistic per-band person count so vids never
    // collide across bands.
    private const val BAND_STRIDE = 1_000_000_000L

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    fun testEventColumns(event: TestEvent): Map<String, ParquetValue> =
      mapOf(
        RawImpressionColumns.PERSON_GENDER to
          parquetValue { stringValue = event.person.gender.name },
        RawImpressionColumns.PERSON_AGE_GROUP to
          parquetValue { stringValue = event.person.ageGroup.name },
      )
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateBenchmarkRawImpressions(), args)
