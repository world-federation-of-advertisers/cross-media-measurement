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

import java.time.Period
import java.time.chrono.ChronoPeriod

object PrivacyLandscape {
  const val PRIVACY_BUCKET_VID_SAMPLE_WIDTH = 1f / 300f

  val datePeriod: ChronoPeriod = Period.ofYears(1)
  val ageGroups: Set<AgeGroup> = AgeGroup.values().toSet()
  val genders = Gender.values().toSet()

  // There are 300 Vid intervals in the range [0, 1). The last interval has a little smaller length
  // - [0.99666667 1) all others have length 1/300
  val vidsIntervalStartPoints: List<Float> = (0..299).map { it * PRIVACY_BUCKET_VID_SAMPLE_WIDTH }
}
