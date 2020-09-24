// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common.identity

import picocli.CommandLine

/**
 * Global singleton listing all the valid duchy ids, configurable by a flag.
 */
object DuchyIds {
  lateinit var ALL: Set<String>
    private set

  val size: Int
    get() = ALL.size

  fun setDuchyIdsFromFlags(duchyIdFlags: DuchyIdFlags) {
    require(!DuchyIds::ALL.isInitialized)
    require(duchyIdFlags.duchyIds.isNotEmpty())
    ALL = duchyIdFlags.duchyIds.toSet()
  }

  fun setDuchyIdsForTest(duchyIds: Set<String>) {
    ALL = duchyIds
  }
}

/**
 * Flag for setting global [DuchyIds.ALL].
 *
 * Usage:
 *   fun run(@CommandLine.Mixin duchyIdFlags: DuchyIdFlags, ...) {
 *     DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)
 *     ...
 *   }
 */
class DuchyIdFlags {
  @CommandLine.Option(
    names = ["--duchy-ids"],
    description = ["List of all valid Duchy ids"],
    split = ",",
    required = true
  )
  lateinit var duchyIds: Array<String>
    private set
}
