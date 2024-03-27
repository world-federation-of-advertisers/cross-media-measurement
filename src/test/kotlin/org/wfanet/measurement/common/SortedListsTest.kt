// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SortedListsTest {
  @Test
  fun `lowerBound throws IllegalArgumentException when the list is empty`() {
    val sortedList = emptyList<Double>()
    val exception = assertFailsWith<IllegalArgumentException> { lowerBound(sortedList, 0.5) }

    assertThat(exception).hasMessageThat().contains("empty")
  }

  @Test
  fun `lowerBound returns the first index of matched value`() {
    val sortedList = listOf(0.1, 0.2, 0.3, 0.3, 0.3, 0.4, 0.5)
    val index = lowerBound(sortedList, 0.3)

    assertThat(index).isEqualTo(2)
  }

  @Test
  fun `lowerBound returns the correct index when the target is not in the list`() {
    val sortedList = listOf(0.1, 0.2, 0.3, 0.3, 0.3, 0.4, 0.5)
    val index1 = lowerBound(sortedList, 0.35)
    assertThat(index1).isEqualTo(5)
  }

  @Test
  fun `lowerBound returns the list size when the target is greater than all elements in the list`() {
    val sortedList = listOf(0.1, 0.2, 0.3, 0.3, 0.3, 0.4, 0.5)
    val index1 = lowerBound(sortedList, 0.55)
    assertThat(index1).isEqualTo(sortedList.size)
  }

  @Test
  fun `upperBound throws IllegalArgumentException when the list is empty`() {
    val sortedList = emptyList<Double>()
    val exception = assertFailsWith<IllegalArgumentException> { upperBound(sortedList, 0.5) }

    assertThat(exception).hasMessageThat().contains("empty")
  }

  @Test
  fun `upperBound returns the index of the first element that is greater than the target`() {
    val sortedList = listOf(0.1, 0.2, 0.3, 0.3, 0.3, 0.4, 0.5)
    val index = upperBound(sortedList, 0.2)

    assertThat(index).isEqualTo(2)
  }

  @Test
  fun `upperBound returns the correct index when the target is not in the list`() {
    val sortedList = listOf(0.1, 0.2, 0.3, 0.3, 0.3, 0.4, 0.5)
    val index1 = upperBound(sortedList, 0.25)
    assertThat(index1).isEqualTo(2)
  }

  @Test
  fun `upperBound returns the list size when the target is greater than or equal to all elements in the list`() {
    val sortedList = listOf(0.1, 0.2, 0.3, 0.3, 0.3, 0.4, 0.5)
    val index1 = upperBound(sortedList, 0.5)
    assertThat(index1).isEqualTo(sortedList.size)
  }
}
