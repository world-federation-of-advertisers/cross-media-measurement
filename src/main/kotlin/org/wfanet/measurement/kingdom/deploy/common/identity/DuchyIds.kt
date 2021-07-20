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

package org.wfanet.measurement.kingdom.deploy.common.identity

import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.internal.kingdom.DuchyIdConfig
import picocli.CommandLine

object DuchyIds {
  lateinit var entries: Array<Entry>
  val count: Int
    get() = DuchyIds.entries.size
  val ALL_DUCHY_EXTERNAL_IDS: Set<String>
    get() = DuchyIds.entries.map { it.externalDuchyId }.toSet()

  fun initializeFromFlags(flags: DuchyIdsFlags) {
    require(!DuchyIds::entries.isInitialized)
    val configMessage =
      flags.config.reader().use { parseTextProto(it, DuchyIdConfig.getDefaultInstance()) }
    require(configMessage.duchiesCount > 0) { "Duchy config has no entries" }
    entries = configMessage.duchiesList.map { it.toDuchyIdsEntry() }.toTypedArray()
  }

  /** Returns the [Entry] for the specified external Duchy ID. */
  fun getByDuchyExternalId(externalDuchyId: String): Entry? {
    return entries.firstOrNull { it.externalDuchyId == externalDuchyId }
  }

  data class Entry(val internalDuchyId: Long, val externalDuchyId: String)
}

class DuchyIdsFlags {
  @CommandLine.Option(
    names = ["--duchy-id-config"],
    description = ["DuchyIdConfig proto message in text format."],
    required = true
  )
  lateinit var config: String
    private set
}

private fun DuchyIdConfig.Duchy.toDuchyIdsEntry(): DuchyIds.Entry {
  return DuchyIds.Entry(internalDuchyId, externalDuchyId)
}
