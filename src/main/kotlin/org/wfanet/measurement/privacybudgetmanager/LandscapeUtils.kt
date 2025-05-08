/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.privacybudgetmanager

/** Wraps utilities to filter and map [PrivacyLandscapes]. */
object LandscapeUtils {
  /** Wraps a landscape to be mapped from and its mapping to another landscape. */
  data class MappingNode(
    val fromLandscape: PrivacyLandscape,
    val mapping: PrivacyLandscapeMapping?,
  )

  /**
   * Filters a list of [PrivacyBucket]s given a [privacyLandscape] and a [eventGroupLandscapeMask]
   * mask to filter it.
   *
   * @param eventGroupLandscapeMasks: Specifies the filters for each event group.
   * @param privacyLandscape: The landscape to be filtered.
   * @returns filtered [PrivacyBucket]s.
   */
  fun getBuckets(
    eventGroupLandscapeMasks: List<EventGroupLandscapeMask>,
    privacyLandscape: PrivacyLandscape,
  ): List<PrivacyBucket> = TODO("uakyol: implement this")

  /**
   * Maps a list of [PrivacyBucket]s from an [fromPrivacyLandscape] to an [toPrivacyLandscape]
   *
   * @param privacyBuckets: [PrivacyBucket] list to be mapped.
   * @param privacyLandscapeMapping: Mapping from the [fromPrivacyLandscape] to
   *   [toPrivacyLandscape].
   * @param fromPrivacyLandscape: The landscape to be mapped from.
   * @param toPrivacyLandscape: The landscape to be mapped to.
   * @returns mapped [PrivacyBucket]s.
   */
  fun mapBuckets(
    privacyBuckets: List<PrivacyBucket>,
    privacyLandscapeMapping: PrivacyLandscapeMapping,
    fromPrivacyLandscape: PrivacyLandscape,
    toPrivacyLandscape: PrivacyLandscape,
  ): List<PrivacyBucket> = TODO("uakyol: implement this")
}
