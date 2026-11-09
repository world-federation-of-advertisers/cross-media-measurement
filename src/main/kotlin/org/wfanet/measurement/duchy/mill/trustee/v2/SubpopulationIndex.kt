/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee.v2

/**
 * The subpopulation of each frequency vector position, and the attribute values of each
 * subpopulation.
 *
 * Subpopulations are numbered `0 until subpopulationCount`.
 */
interface SubpopulationIndex {
  /** Number of subpopulations. */
  val subpopulationCount: Int

  /** Returns the subpopulation of the VID at frequency vector [position]. */
  fun subpopulationOf(position: Int): Int

  /**
   * Returns the enum symbol of the attribute at [path] for [subpopulation], or `null` if that
   * subpopulation does not set it.
   *
   * @param path `<event template name>.<field name>`, e.g. `person.age_group`
   */
  fun attributeValue(subpopulation: Int, path: String): String?
}
