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

package org.wfanet.measurement.common.api

object ResourceIds {
  /**
   * [Regex] matching the resource ID format defined in [AIP-122](https://google.aip.dev/122).
   *
   * This is the same as [RFC_1034_REGEX] without upper-case letters.
   */
  val AIP_122_REGEX = Regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")

  /** [Regex] matching RFC-1034 labels. */
  val RFC_1034_REGEX = Regex("^[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$")
}
