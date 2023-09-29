/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList

/**
 * Terminal operator that awaits for a single value to be emitted.
 *
 * @return the single value, or `null` if the flow is empty
 * @throws IllegalArgumentException for a flow that contains more than one element
 *
 * TODO(@SanjayVas): Move this to common-jvm.
 */
suspend fun <T> Flow<T>.singleOrNullIfEmpty(): T? {
  val results = take(2).toList()
  require(results.size <= 1) { "Flow has more than one element" }
  return results.singleOrNull()
}
