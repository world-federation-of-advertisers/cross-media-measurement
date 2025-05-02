/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.time.Clock
import java.time.ZoneOffset
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.rangeTo

class PrivacyBucketFilter(
  private val privacyBucketMapper: PrivacyBucketMapper,
  private val clock: Clock = Clock.systemUTC(),
) {
  /**
   * Returns a list of privacy bucket groups that might be affected by a query.
   *
   * @param requisitionSpec The requisitionSpec protobuf that is associated with the query. The date
   *   range and demo groups are obtained from this.
   * @param measurementSpec The measurementSpec protobuf that is associated with the query. The VID
   *   sampling interval is obtained from from this.
   * @return A set of potentially affected PrivacyBucketGroups. It is guaranteed that the items in
   *   this list are disjoint. In the current implementation, each privacy bucket group represents a
   *   single privacy bucket.
   */
  fun getPrivacyBucketGroups(
    measurementConsumerId: String,
    privacyLandscapeMask: LandscapeMask,
  ): Set<PrivacyBucketGroup> {

    return privacyLandscapeMask.eventGroupSpecs
      .flatMap {
        getPrivacyBucketGroups(
          measurementConsumerId,
          it,
          privacyLandscapeMask.vidSampleStart,
          privacyLandscapeMask.vidSampleStart + privacyLandscapeMask.vidSampleWidth,
        )
      }
      .toSet()
  }

  private fun getPrivacyBucketGroups(
    measurementConsumerId: String,
    eventSpec: EventGroupSpec,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalEnd: Float,
  ): Sequence<PrivacyBucketGroup> {
    val program = privacyBucketMapper.toPrivacyFilterProgram(eventSpec.eventFilter)

    // TODO(@renjiez): Handle the case that intervals are too small to get any charge.
    // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/2209
    val vidsIntervalStartPoints =
      PrivacyLandscape.vidsIntervalStartPoints.filter {
        if (vidSamplingIntervalEnd > vidSamplingIntervalStart) {
          it in vidSamplingIntervalStart..vidSamplingIntervalEnd
        } else {
          // When interval is wrapping around 1.0, the VID range is [0, vidSamplingIntervalStart] +
          // [vidSamplingIntervalEnd, 1.0]
          it <= vidSamplingIntervalStart || it >= vidSamplingIntervalEnd
        }
      }
    val today = clock.instant().atZone(ZoneOffset.UTC).toLocalDate()
    val dates =
      (today.minus(PrivacyLandscape.datePeriod)..today).filter {
        OpenEndTimeRange.fromClosedDateRange(it..it).overlaps(eventSpec.timeRange)
      }

    return sequence {
      for (vidsIntervalStartPoint in vidsIntervalStartPoints) {
        for (date in dates) {
          for (ageGroup in PrivacyLandscape.ageGroups) {
            for (gender in PrivacyLandscape.genders) {
              val privacyBucketGroup =
                PrivacyBucketGroup(
                  measurementConsumerId,
                  date,
                  date,
                  ageGroup,
                  gender,
                  vidsIntervalStartPoint,
                  PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
                )
              if (privacyBucketMapper.matches(privacyBucketGroup, program)) {
                yield(privacyBucketGroup)
              }
            }
          }
        }
      }
    }
  }
}
