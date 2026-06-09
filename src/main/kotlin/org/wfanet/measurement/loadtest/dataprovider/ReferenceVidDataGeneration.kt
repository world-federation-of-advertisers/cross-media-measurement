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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.hash.Hashing
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.logging.Logger
import kotlin.math.abs
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.ReferenceVidEventGroupSpec
import org.wfanet.measurement.common.LocalDateProgression
import org.wfanet.measurement.common.rangeTo
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.virtualpeople.common.AgeRange
import org.wfanet.virtualpeople.common.DemoBucket
import org.wfanet.virtualpeople.common.DemoInfo
import org.wfanet.virtualpeople.common.EventId
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.ProfileInfo
import org.wfanet.virtualpeople.common.UserInfo
import org.wfanet.virtualpeople.core.labeler.Labeler

object ReferenceVidDataGeneration {
  private val FINGERPRINT_FUNCTION = Hashing.farmHashFingerprint64()
  private const val SECONDS_PER_DAY = 86400

  /** A VID with a timestamp and subpopulation index, produced by the labeler. */
  data class LabeledVid(val timestamp: Instant, val vid: Long, val subPopulationIndex: Int)

  /** A date shard of labeled VIDs. */
  data class LabeledVidDateShard(val localDate: LocalDate, val labeledVids: Sequence<LabeledVid>)

  /** Result of labeling a single reference VID. */
  data class LabeledVidResult(
    val referenceVid: Long,
    val vid: Long,
    val outputGender: Gender,
    val outputMinAge: Int,
    val outputMaxAge: Int,
    val subPopulationIndex: Int,
  )

  /**
   * Generates labeled VIDs by running reference VIDs through a VID [labeler] model.
   *
   * Each reference VID from the spec is converted to a string and passed through the labeler with
   * the demographic profile from the spec. The labeler assigns VIDs via hash-based routing and a
   * demographic correction matrix.
   *
   * The caller is responsible for wrapping the output in event messages (e.g. MarketEvent) using
   * the PopulationSpec attributes.
   */
  fun generateEvents(
    labeler: Labeler,
    populationSpec: PopulationSpec,
    spec: ReferenceVidEventGroupSpec,
    zoneId: ZoneId = ZoneOffset.UTC,
  ): Sequence<LabeledVidDateShard> {
    val rawLabeledVids = labelAllInputs(labeler, spec)
    val labeledVids = validateAgainstPopulationSpec(rawLabeledVids, populationSpec)

    return sequence {
      for (dateSpec in spec.dateSpecsList) {
        val dateProgression = dateSpec.dateRange.toProgression()
        val numDays =
          ChronoUnit.DAYS.between(dateProgression.start, dateProgression.endInclusive) + 1

        for (date in dateProgression) {
          val vids: Sequence<LabeledVid> =
            generateDayVids(
              labeledVids,
              dateProgression,
              date,
              dateSpec.frequency.toInt(),
              numDays.toInt(),
              zoneId,
            )
          yield(LabeledVidDateShard(date, vids))
        }
      }
    }
  }

  private fun labelAllInputs(
    labeler: Labeler,
    spec: ReferenceVidEventGroupSpec,
  ): List<LabeledVidResult> {
    val results = mutableListOf<LabeledVidResult>()

    for (demoDist in spec.demographicDistributionsList) {
      val gender = Gender.forNumber(demoDist.gender) ?: error("Invalid gender: ${demoDist.gender}")

      for (referenceVid in demoDist.idRange.start until demoDist.idRange.endExclusive) {
        val input =
          LabelerInput.newBuilder()
            .apply {
              eventId = EventId.newBuilder().setId(referenceVid.toString()).build()
              timestampUsec = 0L
              profileInfo =
                ProfileInfo.newBuilder()
                  .apply {
                    proprietaryIdSpace1UserInfo =
                      UserInfo.newBuilder()
                        .apply {
                          userId = referenceVid.toString()
                          demo =
                            DemoInfo.newBuilder()
                              .apply {
                                demoBucket =
                                  DemoBucket.newBuilder()
                                    .apply {
                                      this.gender = gender
                                      age =
                                        AgeRange.newBuilder()
                                          .setMinAge(demoDist.minAge)
                                          .setMaxAge(demoDist.maxAge)
                                          .build()
                                    }
                                    .build()
                              }
                              .build()
                        }
                        .build()
                  }
                  .build()
            }
            .build()

        val output = labeler.label(input)
        check(output.peopleCount > 0) {
          "Labeler returned no people for reference VID $referenceVid"
        }
        val person = output.getPeople(0)
        check(person.virtualPersonId > 0) {
          "Labeler returned VID 0 for reference VID $referenceVid"
        }

        results.add(
          LabeledVidResult(
            referenceVid = referenceVid,
            vid = person.virtualPersonId.toLong(),
            outputGender = person.label.demo.gender,
            outputMinAge = person.label.demo.age.minAge,
            outputMaxAge = person.label.demo.age.maxAge,
            subPopulationIndex = -1,
          )
        )
      }
    }

    logger.info(
      "Labeled ${results.size} reference VIDs, " +
        "${results.map { it.vid }.distinct().size} unique VIDs"
    )
    return results
  }

  /**
   * Validates that every labeled VID falls within a [PopulationSpec] subpopulation range and sets
   * the [LabeledVidResult.subPopulationIndex] accordingly.
   */
  private fun validateAgainstPopulationSpec(
    results: List<LabeledVidResult>,
    populationSpec: PopulationSpec,
  ): List<LabeledVidResult> {
    return results.map { result ->
      val (subPopIndex, _) =
        populationSpec.subpopulationsList.withIndex().firstOrNull { (_, sub) ->
          sub.vidRangesList.any { range ->
            result.vid >= range.startVid && result.vid <= range.endVidInclusive
          }
        }
          ?: error(
            "VID ${result.vid} (from reference VID ${result.referenceVid}) " +
              "not in any PopulationSpec range"
          )

      result.copy(subPopulationIndex = subPopIndex)
    }
  }

  private fun generateDayVids(
    labeledVids: List<LabeledVidResult>,
    dateProgression: LocalDateProgression,
    date: LocalDate,
    frequency: Int,
    numDays: Int,
    zoneId: ZoneId,
  ): Sequence<LabeledVid> = sequence {
    val dayNumber = ChronoUnit.DAYS.between(dateProgression.start, date)

    for (result in labeledVids) {
      for (i in 1..frequency) {
        val dayToLog =
          (FINGERPRINT_FUNCTION.hashLong(result.vid * i).asLong() % numDays + numDays) % numDays
        if (dayToLog == dayNumber) {
          val hashInput =
            result.vid
              .toByteString(ByteOrder.BIG_ENDIAN)
              .concat(dayToLog.toByteString(ByteOrder.BIG_ENDIAN))
          val hashValue =
            abs(Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong())
          val impressionTime = date.atStartOfDay(zoneId).plusSeconds(hashValue % SECONDS_PER_DAY)
          yield(LabeledVid(impressionTime.toInstant(), result.vid, result.subPopulationIndex))
        }
      }
    }
  }

  private fun ReferenceVidEventGroupSpec.DateSpec.DateRange.toProgression(): LocalDateProgression {
    return start.toLocalDate()..endExclusive.toLocalDate().minusDays(1)
  }

  private val logger: Logger = Logger.getLogger(this::class.java.name)
}
