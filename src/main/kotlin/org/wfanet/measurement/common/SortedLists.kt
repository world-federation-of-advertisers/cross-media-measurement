/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

// TODO(@ple13): Move this file to commom-jvm.

package org.wfanet.measurement.common

/**
 * Finds the smallest index i such that sortedList[i] >= target.
 *
 * The function takes input as a list of elements of type T. T is a Comparable and has the natural
 * order. The value `sortedList.size` is returned in case all values are less than `target`.
 */
fun <T : Comparable<T>> lowerBound(sortedList: List<T>, target: T): Int {
  require(sortedList.isNotEmpty()) { "Input list cannot be empty." }

  // Obtains the index of the target if there is a match, otherwise return (-insertionPoint - 1).
  var index = sortedList.binarySearch(target)

  // Finds the lower bound index.
  //
  // If there is an exact match, finds the first index where the value is not less than target.
  // Otherwise, the lower bound is the insertion point.
  if (index >= 0) {
    while (index > 0 && sortedList[index - 1] >= target) {
      index--
    }
    return index
  } else {
    return -(index + 1)
  }
}

/**
 * Finds the smallest index i such that sortedList[i] > target.
 *
 * The function takes input as a list of elements of type T. T is a Comparable and has the natural
 * order. The value `sortedList.size` is returned in case all values are less than or equal to
 * `target`.
 */
fun <T : Comparable<T>> upperBound(sortedList: List<T>, target: T): Int {
  require(sortedList.isNotEmpty()) { "Input list cannot be empty." }

  // Obtains the index of the target if there is a match, otherwise return (-insertionPoint - 1).
  var index = sortedList.binarySearch(target)

  // Finds the upper bound index.
  //
  // If there is an exact match, finds the first index where the value is greater than target (if
  // the rest of the list is equal to target, the upper bound is sortedList.size).
  // Otherwise, the lower bound is the insertion point.
  if (index >= 0) {
    while (index < sortedList.size && sortedList[index] <= target) {
      index++
    }
    return index
  } else {
    return -(index + 1)
  }
}
