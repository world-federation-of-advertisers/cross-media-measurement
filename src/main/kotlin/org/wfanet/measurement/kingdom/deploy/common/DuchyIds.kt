// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common

import java.io.File
import java.time.Instant
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.DuchyIdConfig
import picocli.CommandLine

object DuchyIds {
  lateinit var entries: List<Entry>
    private set

  fun initializeFromFlags(flags: DuchyIdsFlags) {
    require(!DuchyIds::entries.isInitialized)
    val configMessage =
      flags.config.reader().use { parseTextProto(it, DuchyIdConfig.getDefaultInstance()) }
    require(configMessage.duchiesCount > 0) { "Duchy Id config has no entries" }
    entries = configMessage.duchiesList.map { it.toDuchyIdsEntry() }
  }

  /**
   * Returns the internalId for the specified external Duchy ID.
   *
   * Note that this performs an O(n) linear scan, where n is the number of Duchies. This should be
   * sufficient as the number of Duchies is expected to be small (~5).
   */
  fun getInternalId(externalDuchyId: String): Long? {
    return entries.firstOrNull { it.externalDuchyId == externalDuchyId }?.internalDuchyId
  }

  /**
   * Returns the externalId for the specified internal Duchy ID.
   *
   * Note that this performs an O(n) linear scan, where n is the number of Duchies. This should be
   * sufficient as the number of Duchies is expected to be small (~5).
   */
  fun getExternalId(internalDuchyId: Long): String? {
    return entries.firstOrNull { it.internalDuchyId == internalDuchyId }?.externalDuchyId
  }

  fun setForTest(duchyIds: List<Entry>) {
    entries = duchyIds
  }

  class Entry(
    val internalDuchyId: Long,
    val externalDuchyId: String,
    val activeRange: ClosedRange<Instant>,
  ) {
    fun isActive(instant: Instant): Boolean {
      return instant in activeRange
    }
  }
}

class DuchyIdsFlags {
  @CommandLine.Option(
    names = ["--duchy-id-config"],
    description = ["DuchyIdConfig proto message in text format."],
    required = true,
  )
  lateinit var config: File
    private set
}

private fun DuchyIdConfig.Duchy.toDuchyIdsEntry(): DuchyIds.Entry {
  val activeEndTime =
    if (hasActiveEndTime()) {
      activeEndTime.toInstant()
    } else {
      Instant.MAX
    }
  return DuchyIds.Entry(
    internalDuchyId,
    externalDuchyId,
    activeStartTime.toInstant()..activeEndTime,
  )
}
