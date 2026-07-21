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

package org.wfanet.virtualpeople.core.model.utils

import com.google.protobuf.fieldMask
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.virtualpeople.common.labelerEvent

@RunWith(JUnit4::class)
class HashFieldMaskMatcherTest {

  @Test
  fun `empty events should throw`() {
    val hashFieldMask = fieldMask { paths.add("person_country_code") }
    val exception =
      assertFailsWith<IllegalStateException> {
        HashFieldMaskMatcher.build(events = listOf(), hashFieldMask)
      }
    assertTrue(exception.message!!.contains("The events is empty"))
  }

  @Test
  fun `empty hash field mask should throw`() {
    val event = labelerEvent { personCountryCode = "COUNTRY_1" }
    val exception =
      assertFailsWith<IllegalStateException> {
        HashFieldMaskMatcher.build(listOf(event), fieldMask {})
      }
    assertTrue(exception.message!!.contains("The hashFieldMask is empty"))
  }

  @Test
  fun `events have same hash should throw`() {
    val event1 = labelerEvent {
      personCountryCode = "COUNTRY_1"
      personRegionCode = "REGION_1"
    }
    val event2 = labelerEvent {
      personCountryCode = "COUNTRY_1"
      personRegionCode = "REGION_2"
    }
    val hashFieldMask = fieldMask { paths.add("person_country_code") }
    val exception =
      assertFailsWith<IllegalStateException> {
        HashFieldMaskMatcher.build(listOf(event1, event2), hashFieldMask)
      }
    assertTrue(exception.message!!.contains("Multiple events have the same hash"))
  }

  @Test
  fun `getMatch return expected results`() {
    val inputEvent1 = labelerEvent {
      personCountryCode = "COUNTRY_1"
      personRegionCode = "REGION_1"
    }
    val inputEvent2 = labelerEvent {
      personCountryCode = "COUNTRY_2"
      personRegionCode = "REGION_2"
    }
    val hashFieldMask = fieldMask { paths.add("person_country_code") }
    val matcher = HashFieldMaskMatcher.build(listOf(inputEvent1, inputEvent2), hashFieldMask)

    /** events[0] matches. Returns 0. */
    val event1 = labelerEvent { personCountryCode = "COUNTRY_1" }
    assertEquals(0, matcher.getMatch(event1))

    /** events[0] matches. Returns 1. person_region_code is ignored. */
    val event2 = labelerEvent {
      personCountryCode = "COUNTRY_1"
      personRegionCode = "REGION_3"
    }
    assertEquals(0, matcher.getMatch(event2))

    /** events[1] matches. Returns 1. */
    val event3 = labelerEvent { personCountryCode = "COUNTRY_2" }
    assertEquals(1, matcher.getMatch(event3))

    /** events[1] matches. Returns 1. person_region_code is ignored. */
    val event4 = labelerEvent {
      personCountryCode = "COUNTRY_2"
      personRegionCode = "REGION_3"
    }
    assertEquals(1, matcher.getMatch(event4))

    /** No matches. Returns -1. */
    val event5 = labelerEvent { personCountryCode = "COUNTRY_3" }
    assertEquals(-1, matcher.getMatch(event5))
  }

  @Test
  fun `unset field in mask matches event unset`() {
    val inputEvent1 = labelerEvent { personRegionCode = "REGION_1" }
    val inputEvent2 = labelerEvent { personRegionCode = "REGION_2" }
    val hashFieldMask = fieldMask {
      paths.add("person_country_code")
      paths.add("person_region_code")
    }
    val matcher = HashFieldMaskMatcher.build(listOf(inputEvent1, inputEvent2), hashFieldMask)

    /** events[0] matches. Returns 0. */
    val event1 = labelerEvent { personRegionCode = "REGION_1" }
    assertEquals(0, matcher.getMatch(event1))

    /**
     * No matches. Returns -1. When using hash field mask, to match an event with unset field, the
     * event being checked must have the same field unset.
     */
    val event2 = labelerEvent {
      personCountryCode = "COUNTRY_1"
      personRegionCode = "REGION_1"
    }
    assertEquals(-1, matcher.getMatch(event2))
  }
}
