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

package org.wfanet.measurement.edpaggregator.benchmark

import com.google.cloud.ByteArray as SpannerByteArray
import java.security.MessageDigest
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.virtualpeople.common.demoInfo
import org.wfanet.virtualpeople.common.geoLocation
import org.wfanet.virtualpeople.common.labelerInput
import org.wfanet.virtualpeople.common.profileInfo
import org.wfanet.virtualpeople.common.userInfo
import org.wfanet.virtualpeople.common.Gender

/** Reach curve fractions: fraction of total reach that is NEW on each day. */
private val NEW_REACH_FRACTIONS = listOf(0.35, 0.21, 0.14, 0.10, 0.08, 0.07, 0.05)

/** Impression fractions: fraction of total impressions delivered on each day. */
private val IMPRESSION_FRACTIONS = listOf(0.20, 0.178, 0.156, 0.144, 0.128, 0.106, 0.089)

data class DayData(
  val day: Int,
  val newAccountIds: List<String>,
  val returningAccountIds: List<String>,
  val totalImpressions: Int,
)

class DataGenerator(val totalReach: Int, val totalImpressions: Int) {

  private val allSeenAccounts = mutableListOf<String>()
  private var nextAccountIndex = 0

  fun generateDay(day: Int): DayData {
    val dayIndex = (day - 1).coerceAtMost(NEW_REACH_FRACTIONS.size - 1)
    val newCount = (totalReach * NEW_REACH_FRACTIONS[dayIndex]).toInt().coerceAtLeast(1)
    val impressionCount =
      (totalImpressions * IMPRESSION_FRACTIONS[dayIndex]).toInt().coerceAtLeast(1)

    val newAccountIds =
      (0 until newCount).map {
        val id = "user_${nextAccountIndex++}"
        allSeenAccounts.add(id)
        id
      }

    val returningCount =
      (impressionCount - newCount).coerceAtLeast(0).coerceAtMost(allSeenAccounts.size - newCount)
    val returningAccountIds =
      if (returningCount > 0 && allSeenAccounts.size > newCount) {
        allSeenAccounts
          .subList(0, allSeenAccounts.size - newCount)
          .shuffled(java.util.Random(day.toLong()))
          .take(returningCount)
      } else {
        emptyList()
      }

    return DayData(
      day = day,
      newAccountIds = newAccountIds,
      returningAccountIds = returningAccountIds,
      totalImpressions = impressionCount,
    )
  }

  companion object {
    fun encryptFingerprint(userId: String): SpannerByteArray {
      val digest = MessageDigest.getInstance("SHA-256")
      return SpannerByteArray.copyFrom(digest.digest(userId.toByteArray(Charsets.UTF_8)))
    }

    fun buildLabelerInput(userId: String, accountIndex: Int): LabelerInput {
      val gender =
        if (accountIndex % 2 == 0) Gender.GENDER_FEMALE else Gender.GENDER_MALE
      val minAge: Int
      val maxAge: Int
      when (accountIndex % 3) {
        0 -> { minAge = 18; maxAge = 34 }
        1 -> { minAge = 35; maxAge = 54 }
        else -> { minAge = 55; maxAge = 65 }
      }
      val countryId = if (accountIndex % 2 == 0) 1 else 2

      return labelerInput {
        profileInfo = profileInfo {
          emailUserInfo = userInfo {
            this.userId = userId
            demo = demoInfo {
              demoBucket = demoBucket {
                this.gender = gender
                age = ageRange {
                  this.minAge = minAge
                  this.maxAge = maxAge
                }
              }
            }
          }
        }
        geo = geoLocation { this.countryId = countryId }
      }
    }
  }
}
