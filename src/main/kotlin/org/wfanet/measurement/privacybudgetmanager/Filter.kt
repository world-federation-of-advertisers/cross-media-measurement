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

/** Wraps utilities to filter [PrivacyLandscapes] based on [EventGroupLandscapeMask]s. */
object Filter {
  /**
   * Filters a list of [PrivacyBucket]s given a [privacyLandscape] and a [eventGroupLandscapeMask]
   * mask to filter it.
   */
  fun getBuckets(
    eventGroupLandscapeMask: EventGroupLandscapeMask,
    privacyLandscape: PrivacyLandscape,
  ): List<PrivacyBucket> = TODO("uakyol: implement this")

  /**
   * Filters a list of [PrivacyBucket]s given a [inactiveprivacyLandscape] with a
   * [eventGroupLandscapeMask]. Then converts these to new buckets that adheres to
   * [ActivePrivacyLandsapce] by using [privacyLandscapeMapping].
   */
  fun getBuckets(
    eventGroupLandscapeMask: EventGroupLandscapeMask,
    privacyLandscapeMapping: PrivacyLandscapeMapping,
    inactiveprivacyLandscape: PrivacyLandscape,
    activeprivacyLandscape: PrivacyLandscape,
  ): List<PrivacyBucket> = TODO("uakyol: implement this")
}
