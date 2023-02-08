// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.config

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing

private const val TEMPLATE_PREFIX = "wfa.measurement.api.v2alpha.event_templates.testing"

object EventFilters {

  /**
   * Values of this map are anded to create the event filter to be sent to the EDPs.
   *
   * For purposes of this simulation, all of the EDPs register the same templates and receive the
   * same filter from the MC.
   */
  val EVENT_TEMPLATES_TO_FILTERS_MAP =
    mapOf(
      "$TEMPLATE_PREFIX.Person" to
        "person.age_group.value == $TEMPLATE_PREFIX.Person.AgeGroup.YEARS_18_TO_34"
    )

  val VID_SAMPLER_HASH_FUNCTION: HashFunction = Hashing.farmHashFingerprint64()
}
