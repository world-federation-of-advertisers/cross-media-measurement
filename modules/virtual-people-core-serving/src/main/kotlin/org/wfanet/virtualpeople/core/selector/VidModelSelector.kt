// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.selector

import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import kotlin.math.abs
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toLong
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.core.common.Hashing
import org.wfanet.virtualpeople.core.selector.resourcekey.ModelLineKey
import org.wfanet.virtualpeople.core.selector.resourcekey.ModelRolloutKey

const val CACHE_SIZE = 60
const val UPPER_BOUND_PERCENTAGE_ADOPTION = 1.1

/**
 * A utility class to determine what VID Model must be used to label events.
 *
 * Each ModelLine uses a different instance of this class. In case either the ModelLine or the list
 * of ModelRollouts change, a new instance must be created.
 *
 * @property modelLine the ModelLine used to determine what ModelRelease must be used to label
 *   events
 * @property rollouts the list of rollouts contained in the modelLine
 */
class VidModelSelector(private val modelLine: ModelLine, private val rollouts: List<ModelRollout>) {
  init {
    val modelLineId = ModelLineKey.fromName(modelLine.name)?.modelLineId
    require(modelLineId != null) { "ModelLine resource name is either unspecified or invalid" }
    for (rollout in rollouts) {
      val modelRolloutModelLineId = ModelRolloutKey.fromName(rollout.name)?.modelLineId
      require(modelLineId == modelRolloutModelLineId) {
        "ModelRollouts must be parented by the provided ModelLine."
      }
    }
  }

  /**
   * The adoption percentage of models is calculated each day. It consists of an array of
   * ModelReleasePercentile where each ModelReleasePercentile wraps the percentage of adoption of a
   * particular ModelRelease and the ModelRelease itself. LruCache keys are LocalDate objects
   * expressed in UTC time.
   *
   * Elements in the ArrayList are sorted by ModelRollout rollout start time from the oldest to the
   * most recent.
   */
  private val lruCache: LruCache<LocalDate, List<ModelReleasePercentile>> = LruCache(CACHE_SIZE)

  fun getModelRelease(labelerInput: LabelerInput): String? {
    val eventTimestampSec = labelerInput.timestampUsec / 1_000_000L
    val eventInstant = Instant.ofEpochSecond(eventTimestampSec)
    val modelLineActiveRange =
      OpenEndTimeRange(modelLine.activeStartTime.toInstant(), modelLine.activeEndTime.toInstant())
    if (eventInstant in modelLineActiveRange) {
      val eventDateUtc = eventInstant.atZone(ZoneOffset.UTC).toLocalDate()
      val modelAdoptionPercentages = readFromCache(eventDateUtc)
      var selectedModelRelease =
        if (!modelAdoptionPercentages.isEmpty()) {
          modelAdoptionPercentages.get(0).modelReleaseResourceKey
        } else {
          return null
        }
      val eventId = getEventId(labelerInput)
      for (percentage in modelAdoptionPercentages) {
        val eventFingerprint =
          Hashing.hashFingerprint64(
              buildString {
                append(percentage.modelReleaseResourceKey)
                append(eventId)
              }
            )
            .toLong(ByteOrder.LITTLE_ENDIAN)
        val reducedEventId = abs(eventFingerprint.toDouble() / Long.MAX_VALUE)
        if (reducedEventId < percentage.endPercentile) {
          selectedModelRelease = percentage.modelReleaseResourceKey
        }
      }
      return selectedModelRelease
    }
    return null
  }

  /**
   * Access to the cache is synchronized to prevent multiple threads calculating percentages in case
   * of cache miss.
   */
  private fun readFromCache(eventDateUtc: LocalDate): List<ModelReleasePercentile> {
    synchronized(this) {
      if (lruCache.containsKey(eventDateUtc)) {
        return lruCache[eventDateUtc]!!
      } else {
        val percentages = calculatePercentages(eventDateUtc)
        lruCache[eventDateUtc] = percentages
        return percentages
      }
    }
  }

  /**
   * Return a list of ModelReleasePercentile(s). Each ModelReleasePercentile wraps the percentage of
   * adoption of a particular ModelRelease and the ModelRelease itself. The list is sorted by either
   * rollout_period_start_date or instant_rollout_date.
   *
   * The adoption percentage of each ModelRollout is calculated as follows: (EVENT_DAY
   * - ROLLOUT_START_DAY) / (ROLLOUT_END_DAY - ROLLOUT_START_DAY).
   *
   * In case a ModelRollout has the `rollout_freeze_date` set and the event day is greater than
   * rollout_freeze_date, the EVENT_DAY in the above formula is replaced by `rollout_freeze_date` to
   * ensure that the rollout stops its expansion: (ROLLOUT_FREEZE_DATE - ROLLOUT_START_DAY) /
   * (ROLLOUT_END_DAY - ROLLOUT_START_DAY).
   *
   * In case of an instant rollout ROLLOUT_START_DATE is equal to ROLLOUT_END_DATE.
   */
  private fun calculatePercentages(eventDateUtc: LocalDate): List<ModelReleasePercentile> {
    val activeRollouts: List<ModelRollout> = retrieveActiveRollouts(eventDateUtc)
    return activeRollouts.map { activeRollout ->
      ModelReleasePercentile(
        calculatePercentageAdoption(eventDateUtc, activeRollout),
        activeRollout.modelRelease
      )
    }
  }

  /**
   * Returns the percentage of events that this ModelRollout must label for the given `eventDateUtc`
   */
  private fun calculatePercentageAdoption(
    eventDateUtc: LocalDate,
    modelRollout: ModelRollout
  ): Double {
    val modelRolloutFreezeDate =
      if (modelRollout.hasRolloutFreezeDate()) {
        modelRollout.rolloutFreezeDate.toLocalDate()
      } else {
        LocalDate.MAX
      }

    val rolloutPeriodStartDate =
      if (modelRollout.hasGradualRolloutPeriod()) {
        modelRollout.gradualRolloutPeriod.startDate.toLocalDate()
      } else {
        modelRollout.instantRolloutDate.toLocalDate()
      }
    val rolloutPeriodEndDate =
      if (modelRollout.hasGradualRolloutPeriod()) {
        modelRollout.gradualRolloutPeriod.endDate.toLocalDate()
      } else {
        modelRollout.instantRolloutDate.toLocalDate()
      }

    return if (rolloutPeriodStartDate == rolloutPeriodEndDate) {
      UPPER_BOUND_PERCENTAGE_ADOPTION
    } else if (eventDateUtc >= modelRolloutFreezeDate) {
      (ChronoUnit.DAYS.between(rolloutPeriodStartDate, modelRolloutFreezeDate).toDouble()) /
        (ChronoUnit.DAYS.between(rolloutPeriodStartDate, rolloutPeriodEndDate).toDouble())
    } else {
      (ChronoUnit.DAYS.between(rolloutPeriodStartDate, eventDateUtc).toDouble()) /
        (ChronoUnit.DAYS.between(rolloutPeriodStartDate, rolloutPeriodEndDate).toDouble())
    }
  }

  /**
   * Iterates through all available ModelRollout(s) sorted by rollout_period_start_time from the
   * most recent to the oldest. The function keeps adding ModelRollout(s) to the `activeRollouts`
   * array until the following condition is met: eventDay >= rolloutPeriodEndTime &&
   * !rollout.hasRolloutFreezeTime()
   */
  private fun retrieveActiveRollouts(eventDateUtc: LocalDate): List<ModelRollout> {
    if (rollouts.isEmpty()) {
      return rollouts
    }
    val sortedRollouts: List<ModelRollout> =
      rollouts.sortedBy {
        if (it.hasGradualRolloutPeriod()) {
          it.gradualRolloutPeriod.startDate.toLocalDate()
        } else {
          it.instantRolloutDate.toLocalDate()
        }
      }
    val firstRolloutPeriodStartDate =
      if (sortedRollouts.first().hasGradualRolloutPeriod()) {
        sortedRollouts.first().gradualRolloutPeriod.startDate
      } else {
        sortedRollouts.first().instantRolloutDate
      }
    if (eventDateUtc < firstRolloutPeriodStartDate.toLocalDate()) {
      return emptyList()
    }
    val activeRollouts = mutableListOf<ModelRollout>()
    for (rollout in sortedRollouts.asReversed()) {
      val rolloutPeriodEndDate =
        if (rollout.hasGradualRolloutPeriod()) {
          rollout.gradualRolloutPeriod.endDate.toLocalDate()
        } else {
          rollout.instantRolloutDate.toLocalDate()
        }
      if (eventDateUtc >= rolloutPeriodEndDate) {
        activeRollouts.add(rollout)
        if (!rollout.hasRolloutFreezeDate()) {
          // Stop only if there is no Freeze time set, otherwise other rollouts are taken until one
          // without rollout_freeze_time is found or no other rollouts are available
          break
        }
        continue
      }
      val rolloutPeriodStartDate =
        if (rollout.hasGradualRolloutPeriod()) {
          rollout.gradualRolloutPeriod.startDate.toLocalDate()
        } else {
          rollout.instantRolloutDate.toLocalDate()
        }
      if (eventDateUtc >= rolloutPeriodStartDate) {
        activeRollouts.add(rollout)
      }
    }
    return activeRollouts.asReversed()
  }

  private fun getEventId(labelerInput: LabelerInput): String {
    if (labelerInput.hasProfileInfo()) {
      val profileInfo = labelerInput.profileInfo
      if (profileInfo.emailUserInfo.hasUserId()) {
        return profileInfo.emailUserInfo.userId
      }
      if (profileInfo.phoneUserInfo.hasUserId()) {
        return profileInfo.phoneUserInfo.userId
      }
      if (profileInfo.loggedInIdUserInfo.hasUserId()) {
        return profileInfo.loggedInIdUserInfo.userId
      }
      if (profileInfo.loggedOutIdUserInfo.hasUserId()) {
        return profileInfo.loggedOutIdUserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace1UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace1UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace2UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace2UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace3UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace3UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace4UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace4UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace5UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace5UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace6UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace6UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace7UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace7UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace8UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace8UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace9UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace9UserInfo.userId
      }
      if (profileInfo.proprietaryIdSpace10UserInfo.hasUserId()) {
        return profileInfo.proprietaryIdSpace10UserInfo.userId
      }
    } else if (labelerInput.eventId.hasId()) {
      return labelerInput.eventId.id
    }
    error("Neither user_id nor event_id was found in the LabelerInput.")
  }

  private data class ModelReleasePercentile(
    val endPercentile: Double,
    val modelReleaseResourceKey: String,
  )
}
