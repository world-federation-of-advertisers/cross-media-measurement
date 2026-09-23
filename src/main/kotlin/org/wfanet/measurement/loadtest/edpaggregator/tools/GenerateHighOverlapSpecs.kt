/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.TextFormat
import com.google.protobuf.TypeRegistry
import com.google.protobuf.struct
import com.google.protobuf.value
import com.google.type.date
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.Locale
import kotlin.math.roundToInt
import kotlin.system.exitProcess
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.measurement.integration.k8s.testing.ImpressionTestDataConfigKt.entityKeySpec
import org.measurement.integration.k8s.testing.ImpressionTestDataConfigKt.syntheticEventGroup
import org.measurement.integration.k8s.testing.impressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.fieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.populationSpec

/**
 * Generates the high overlap synthetic data specs.
 *
 * The reached VID space is partitioned into one segment per primitive Venn region across four
 * aggregator EDPs, plus a deliberately sub-sigma noise probe. A segment is seeded on exactly the
 * EDPs named in its region, so selecting the whole menu lights up all 15 regions at once.
 *
 * Demographics are orthogonal to the Venn structure: the 30 (gender, age_group, us_state) tuples
 * are interleaved in [STRIPE_SIZE]-VID stripes cycling round-robin, so one full cycle fits inside
 * the smallest region and every region carries the full demographic mix.
 *
 * Usage: `GenerateHighOverlapSpecs <output-directory>`
 */
object GenerateHighOverlapSpecs {

  private const val STRIPE_SIZE = 10_000
  private const val TOTAL_POPULATION = 360_000_000L

  /** Highest VID any segment reaches, rounded up to a whole stripe. */
  private const val REACHED_VID_END = 10_610_000L

  private const val EDP7 = "edp7"
  private const val META = "edpa_meta"
  private const val VIDEO = "edpa_video_pub"
  private const val RETAIL = "edpa_retail_pub"

  /** The date range over which a segment's impressions are spread. */
  private data class Flight(val start: LocalDate, val endExclusive: LocalDate)

  /** A frequency and its share of a tranche's VIDs. */
  private data class FrequencyShare(val frequency: Int, val share: Double)

  private val FLIGHT_SPRING = Flight(LocalDate.of(2026, 4, 1), LocalDate.of(2026, 5, 15))
  private val FLIGHT_SUMMER = Flight(LocalDate.of(2026, 5, 15), LocalDate.of(2026, 7, 1))
  private val FLIGHT_ALWAYS_ON = Flight(LocalDate.of(2026, 4, 1), LocalDate.of(2026, 7, 1))

  private const val TRANCHES = 6

  /** Mean frequency 2.1, monotonic K+ curve. */
  private val FREQUENCY_MIX =
    listOf(
      FrequencyShare(frequency = 1, share = 0.40),
      FrequencyShare(frequency = 2, share = 0.30),
      FrequencyShare(frequency = 3, share = 0.20),
      FrequencyShare(frequency = 5, share = 0.10),
    )

  /** Engagement values cycled per block so threshold IQFs select real subsets. */
  private val COMPLETED_FRACTIONS = listOf(0.0f, 0.25f, 0.5f, 0.75f, 1.0f)
  private val VIEWABLE_FRACTIONS = listOf(0.0f, 0.5f, 1.0f)

  private val GENDERS = listOf(Common.Gender.MALE, Common.Gender.FEMALE)
  private val AGE_GROUPS =
    listOf(
      Common.AgeGroup.YEARS_18_TO_34,
      Common.AgeGroup.YEARS_35_TO_54,
      Common.AgeGroup.YEARS_55_PLUS,
    )
  private val US_STATES =
    listOf(
      Common.UsState.CALIFORNIA,
      Common.UsState.NEW_YORK,
      Common.UsState.TEXAS,
      Common.UsState.FLORIDA,
      Common.UsState.ILLINOIS,
    )

  private data class EntityMetadata(val brand: String, val campaign: String, val placement: String)

  private data class Segment(
    val name: String,
    val vidStart: Long,
    val vidCount: Long,
    val edps: List<String>,
    val entityType: String,
    val entityCount: Int,
    val flight: Flight,
    val metadata: EntityMetadata,
  )

  private val SEGMENTS =
    listOf(
      Segment(
        name = "e7only",
        vidStart = 1,
        vidCount = 2_000_000,
        edps = listOf(EDP7),
        entityType = "campaign",
        entityCount = 1,
        flight = FLIGHT_ALWAYS_ON,
        metadata = EntityMetadata("brand-a", "always-on", "feed"),
      ),
      Segment(
        name = "metaonly",
        vidStart = 2_000_001,
        vidCount = 1_600_000,
        edps = listOf(META),
        entityType = "ad_group",
        entityCount = 3,
        flight = FLIGHT_SPRING,
        metadata = EntityMetadata("brand-a", "spring-launch", "instream"),
      ),
      Segment(
        name = "videoonly",
        vidStart = 3_600_001,
        vidCount = 1_200_000,
        edps = listOf(VIDEO),
        entityType = "creative-id",
        entityCount = 3,
        flight = FLIGHT_SPRING,
        metadata = EntityMetadata("brand-a", "spring-launch", "homepage"),
      ),
      Segment(
        name = "retailonly",
        vidStart = 4_800_001,
        vidCount = 1_000_000,
        edps = listOf(RETAIL),
        entityType = "campaign",
        entityCount = 1,
        flight = FLIGHT_SUMMER,
        metadata = EntityMetadata("brand-a", "summer-sale", "feed"),
      ),
      Segment(
        name = "e7-meta",
        vidStart = 5_800_001,
        vidCount = 800_000,
        edps = listOf(EDP7, META),
        entityType = "ad_group",
        entityCount = 1,
        flight = FLIGHT_SPRING,
        metadata = EntityMetadata("brand-b", "spring-launch", "feed"),
      ),
      Segment(
        name = "e7-video",
        vidStart = 6_600_001,
        vidCount = 600_000,
        edps = listOf(EDP7, VIDEO),
        entityType = "creative-id",
        entityCount = 2,
        flight = FLIGHT_ALWAYS_ON,
        metadata = EntityMetadata("brand-b", "always-on", "homepage"),
      ),
      Segment(
        name = "e7-retail",
        vidStart = 7_200_001,
        vidCount = 400_000,
        edps = listOf(EDP7, RETAIL),
        entityType = "campaign",
        entityCount = 1,
        flight = FLIGHT_SUMMER,
        metadata = EntityMetadata("brand-b", "summer-sale", "feed"),
      ),
      Segment(
        name = "meta-video",
        vidStart = 7_600_001,
        vidCount = 500_000,
        edps = listOf(META, VIDEO),
        entityType = "ad_group",
        entityCount = 3,
        flight = FLIGHT_SPRING,
        metadata = EntityMetadata("brand-a", "spring-launch", "instream"),
      ),
      Segment(
        name = "meta-retail",
        vidStart = 8_100_001,
        vidCount = 300_000,
        edps = listOf(META, RETAIL),
        entityType = "campaign",
        entityCount = 1,
        flight = FLIGHT_SUMMER,
        metadata = EntityMetadata("brand-b", "summer-sale", "feed"),
      ),
      Segment(
        name = "video-retail",
        vidStart = 8_400_001,
        vidCount = 400_000,
        edps = listOf(VIDEO, RETAIL),
        entityType = "creative-id",
        entityCount = 2,
        flight = FLIGHT_ALWAYS_ON,
        metadata = EntityMetadata("brand-a", "always-on", "homepage"),
      ),
      Segment(
        name = "e7-meta-video",
        vidStart = 8_800_001,
        vidCount = 500_000,
        edps = listOf(EDP7, META, VIDEO),
        entityType = "campaign",
        entityCount = 1,
        flight = FLIGHT_SPRING,
        metadata = EntityMetadata("brand-a", "spring-launch", "feed"),
      ),
      Segment(
        name = "e7-meta-retail",
        vidStart = 9_300_001,
        vidCount = 300_000,
        edps = listOf(EDP7, META, RETAIL),
        entityType = "ad_group",
        entityCount = 1,
        flight = FLIGHT_SUMMER,
        metadata = EntityMetadata("brand-b", "summer-sale", "instream"),
      ),
      Segment(
        name = "e7-video-retail",
        vidStart = 9_600_001,
        vidCount = 300_000,
        edps = listOf(EDP7, VIDEO, RETAIL),
        entityType = "creative-id",
        entityCount = 1,
        flight = FLIGHT_ALWAYS_ON,
        metadata = EntityMetadata("brand-b", "always-on", "homepage"),
      ),
      Segment(
        name = "meta-video-retail",
        vidStart = 9_900_001,
        vidCount = 400_000,
        edps = listOf(META, VIDEO, RETAIL),
        entityType = "campaign",
        entityCount = 1,
        flight = FLIGHT_SPRING,
        metadata = EntityMetadata("brand-a", "spring-launch", "feed"),
      ),
      Segment(
        name = "all4",
        vidStart = 10_300_001,
        vidCount = 300_000,
        edps = listOf(EDP7, META, VIDEO, RETAIL),
        entityType = "ad_group",
        entityCount = 3,
        flight = FLIGHT_ALWAYS_ON,
        metadata = EntityMetadata("brand-a", "always-on", "instream"),
      ),
      // Sub-sigma noise probe. Sized well under sigma so noise-dominated reports get flagged.
      // At 10x epsilon sigma is ~18k, so 8k is ~0.4 sigma.
      Segment(
        name = "noise",
        vidStart = 10_600_001,
        vidCount = 8_000,
        edps = listOf(EDP7),
        entityType = "ad_group",
        entityCount = 1,
        flight = FLIGHT_SUMMER,
        metadata = EntityMetadata("brand-b", "summer-sale-promo", "feed"),
      ),
    )

  private const val POPULATION_SPEC_FILE = "high_overlap_population_spec.textproto"
  private const val CONFIG_FILE = "high_overlap_impression_test_data_config.textproto"

  private val TYPE_REGISTRY: TypeRegistry =
    TypeRegistry.newBuilder().add(Common.getDescriptor()).build()

  /** The 30 (gender, age_group, us_state) tuples, in a fixed order. */
  private val DEMOGRAPHIC_TUPLES: List<Common> =
    GENDERS.flatMap { gender ->
      AGE_GROUPS.flatMap { ageGroup ->
        US_STATES.map { state ->
          common {
            this.gender = gender
            this.ageGroup = ageGroup
            usState = state
          }
        }
      }
    }

  /**
   * Name of the spec file for one entity key of a segment.
   *
   * Segments with a single entity key are unsuffixed.
   */
  private fun segmentFileName(segment: Segment, entityIndex: Int): String {
    val base = "high_overlap_seg_${segment.name.replace('-', '_')}"
    return if (segment.entityCount == 1) "$base.textproto"
    else "${base}_${entityIndex + 1}.textproto"
  }

  /**
   * The VID slice one entity key covers.
   *
   * A segment's entity keys partition its VIDs rather than each carrying the whole segment, so
   * impressions and frequency are not multiplied by the entity count. Slices tile the segment
   * exactly; a boundary falling mid-stripe is fine, since [buildSegmentSpec] clamps every
   * `vid_range_spec` to its enclosing stripe.
   */
  private fun entityKeySlice(segment: Segment, entityIndex: Int): VidRange =
    VidRange(
      segment.vidStart + segment.vidCount * entityIndex / segment.entityCount,
      segment.vidStart + segment.vidCount * (entityIndex + 1) / segment.entityCount,
    )

  /** Formats [value] with thousands separators, for VID ranges in descriptions. */
  private fun grouped(value: Long): String = String.format(Locale.ROOT, "%,d", value)

  /** Builds the population: demographic tuples interleaved in stripes, plus an unreached filler. */
  private fun buildPopulationSpec(): PopulationSpec {
    val stripeCount = (REACHED_VID_END / STRIPE_SIZE).toInt()
    return populationSpec {
      for (i in 0 until stripeCount) {
        subpopulations +=
          PopulationSpecKt.subPopulation {
            vidRanges +=
              PopulationSpecKt.vidRange {
                startVid = i.toLong() * STRIPE_SIZE + 1
                endVidInclusive = (i.toLong() + 1) * STRIPE_SIZE
              }
            attributes += ProtoAny.pack(DEMOGRAPHIC_TUPLES[i % DEMOGRAPHIC_TUPLES.size])
          }
      }
      // Never reached; holds populationSpec.size at TOTAL_POPULATION so frequency-vector
      // allocation is stressed.
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = REACHED_VID_END + 1
              endVidInclusive = TOTAL_POPULATION
            }
          attributes += ProtoAny.pack(DEMOGRAPHIC_TUPLES[stripeCount % DEMOGRAPHIC_TUPLES.size])
        }
    }
  }

  /** A half-open VID range. */
  private data class VidRange(val start: Long, val endExclusive: Long)

  /** Key grouping the blocks that share a tranche and a frequency. */
  private data class TrancheFrequency(val tranche: Int, val frequency: Int)

  private data class Block(val start: Long, val endExclusive: Long, val video: Boolean)

  /**
   * Builds the spec for one entity key of a segment, covering that key's VID slice.
   *
   * Every `vid_range_spec` is exactly one population stripe: `SyntheticDataGeneration` resolves a
   * range's demographics by finding the single subpopulation wholly containing it, so a range
   * spanning two stripes fails. Slice boundaries are multiples of [STRIPE_SIZE], so stripes tile
   * each slice exactly.
   *
   * Stripes are assigned round-robin to (tranche, frequency, media): tranches stagger first
   * exposure across the flight so cumulative reach grows week over week, the frequency mix gives a
   * non-flat monotonic K+ curve, and alternating media means every slice emits both VIDEO and
   * DISPLAY.
   */
  private fun buildSegmentSpec(segment: Segment, entityIndex: Int): SyntheticEventGroupSpec {
    val flightStart = segment.flight.start
    val flightEnd = segment.flight.endExclusive
    val flightDays = ChronoUnit.DAYS.between(flightStart, flightEnd)

    val vidSlice = entityKeySlice(segment, entityIndex)
    val stripes = mutableListOf<VidRange>()
    var cursor = vidSlice.start
    val vidEnd = vidSlice.endExclusive
    while (cursor < vidEnd) {
      val end = minOf(cursor + STRIPE_SIZE, vidEnd)
      // Clamp to the enclosing stripe boundary so a range never straddles two.
      val boundary = ((cursor - 1) / STRIPE_SIZE + 1) * STRIPE_SIZE + 1
      val stripeEnd = minOf(end, boundary)
      stripes.add(VidRange(cursor, stripeEnd))
      cursor = stripeEnd
    }

    val trancheCount = minOf(TRANCHES, stripes.size)
    // Expand the frequency mix into a repeating pattern of the right proportions.
    val pattern =
      FREQUENCY_MIX.flatMap { frequencyShare ->
        List(maxOf(1, (frequencyShare.share * 10).roundToInt())) { frequencyShare.frequency }
      }

    val blocks = linkedMapOf<TrancheFrequency, MutableList<Block>>()
    stripes.forEachIndexed { i, stripe ->
      val tranche = (i * trancheCount) / stripes.size
      val frequency = pattern[i % pattern.size]
      blocks
        .getOrPut(TrancheFrequency(tranche, frequency)) { mutableListOf() }
        .add(Block(stripe.start, stripe.endExclusive, i % 2 == 0))
    }

    return syntheticEventGroupSpec {
      description =
        "high overlap segment ${segment.name}: VIDs ${grouped(vidSlice.start)}-" +
          "${grouped(vidEnd - 1)} over $flightStart..$flightEnd"
      for (tranche in 0 until trancheCount) {
        val frequencies = blocks.keys.filter { it.tranche == tranche }.map { it.frequency }.sorted()
        if (frequencies.isEmpty()) continue
        val startDate = flightStart.plusDays(flightDays * tranche / trancheCount)
        dateSpecs +=
          SyntheticEventGroupSpecKt.dateSpec {
            dateRange =
              SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
                start = startDate.toProtoDate()
                endExclusive = flightEnd.toProtoDate()
              }
            for (frequency in frequencies) {
              frequencySpecs +=
                SyntheticEventGroupSpecKt.frequencySpec {
                  this.frequency = frequency.toLong()
                  blocks.getValue(TrancheFrequency(tranche, frequency)).forEachIndexed { n, block ->
                    vidRangeSpecs +=
                      SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                        vidRange = vidRange {
                          start = block.start
                          endExclusive = block.endExclusive
                        }
                        if (block.video) {
                          nonPopulationFieldValues["video.completed_fraction"] = fieldValue {
                            floatValue = COMPLETED_FRACTIONS[n % COMPLETED_FRACTIONS.size]
                          }
                          nonPopulationFieldValues["video.viewable_fraction"] = fieldValue {
                            floatValue = VIEWABLE_FRACTIONS[n % VIEWABLE_FRACTIONS.size]
                          }
                        } else {
                          nonPopulationFieldValues["display.viewable_fraction"] = fieldValue {
                            floatValue = VIEWABLE_FRACTIONS[(n + 1) % VIEWABLE_FRACTIONS.size]
                          }
                        }
                      }
                  }
                }
            }
          }
      }
    }
  }

  /** Builds the config: one entry per (segment, EDP) pair. */
  private fun buildConfig(): ImpressionTestDataConfig = impressionTestDataConfig {
    populationSpecResourcePath = POPULATION_SPEC_FILE
    for (segment in SEGMENTS) {
      for (edp in segment.edps) {
        eventGroups += syntheticEventGroup {
          eventGroupReferenceId = "high-overlap-${segment.name}-$edp"
          edpName = edp
          outputBasePath = "edp/$edp"
          outputKey = "high-overlap-${segment.name}"
          for (i in 0 until segment.entityCount) {
            val suffix = if (segment.entityCount == 1) "" else "-${i + 1}"
            entityKeySpecs += entityKeySpec {
              entityType = segment.entityType
              entityId = "high-overlap-${segment.name}-$edp$suffix"
              dataSpecResourcePath = segmentFileName(segment, i)
            }
          }
          entityMetadata = struct {
            fields["brand"] = value { stringValue = segment.metadata.brand }
            fields["campaign_name"] = value { stringValue = segment.metadata.campaign }
            fields["placement"] = value { stringValue = segment.metadata.placement }
          }
        }
      }
    }
  }

  private fun LocalDate.toProtoDate(): com.google.type.Date = date {
    year = this@toProtoDate.year
    month = this@toProtoDate.monthValue
    day = this@toProtoDate.dayOfMonth
  }

  private fun write(outputDir: File, fileName: String, message: com.google.protobuf.Message) {
    val printer = TextFormat.printer().usingTypeRegistry(TYPE_REGISTRY)
    File(outputDir, fileName).writeText(printer.printToString(message))
  }

  @JvmStatic
  fun main(args: Array<String>) {
    if (args.size != 1) {
      System.err.println("Usage: GenerateHighOverlapSpecs <output-directory>")
      exitProcess(1)
    }
    val outputDir = Paths.get(args[0]).toFile()
    check(outputDir.isDirectory || outputDir.mkdirs()) { "Cannot create ${outputDir.path}" }

    write(outputDir, POPULATION_SPEC_FILE, buildPopulationSpec())
    for (segment in SEGMENTS) {
      for (i in 0 until segment.entityCount) {
        write(outputDir, segmentFileName(segment, i), buildSegmentSpec(segment, i))
      }
    }
    write(outputDir, CONFIG_FILE, buildConfig())
  }
}
