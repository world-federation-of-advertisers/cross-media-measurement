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

import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.internal.kingdom.ProtocolConfigIdConfig
import picocli.CommandLine

object ProtocolConfigIds {
  lateinit var entries: List<Entry>
    private set

  fun initializeFromFlags(flags: ProtocolConfigIdsFlags) {
    require(!ProtocolConfigIds::entries.isInitialized)
    val configMessage =
      flags.config.reader().use { parseTextProto(it, ProtocolConfigIdConfig.getDefaultInstance()) }
    require(configMessage.protocolConfigsCount > 0) { "ProtocolConfig Id config has no entries" }
    entries = configMessage.protocolConfigsList.map { Entry(it.internalId, it.externalId) }
  }

  /**
   * Returns the internalId for the specified external ProtocolConfig ID.
   *
   * Note that this performs an O(n) linear scan, where n is the number of ProtocolConfigs. This
   * should be sufficient as the number of ProtocolConfigs is expected to be small (<10).
   */
  fun getInternalId(externalId: String): Long? {
    return entries.firstOrNull { it.externalId == externalId }?.internalId
  }

  /**
   * Returns the externalId for the specified internal ProtocolConfig ID.
   *
   * Note that this performs an O(n) linear scan, where n is the number of ProtocolConfigs. This
   * should be sufficient as the number of ProtocolConfigs is expected to be small (<10).
   */
  fun getExternalId(internalId: Long): String? {
    return entries.firstOrNull { it.internalId == internalId }?.externalId
  }

  fun setForTest(protocolConfigIds: List<String>) {
    require(!ProtocolConfigIds::entries.isInitialized)
    entries = protocolConfigIds.mapIndexed { idx, value -> Entry(idx.toLong(), value) }
  }

  data class Entry(val internalId: Long, val externalId: String)
}

class ProtocolConfigIdsFlags {
  @CommandLine.Option(
    names = ["--protocol-config-id-config"],
    description = ["ProtocolConfigIdConfig proto message in text format."],
    required = true
  )
  lateinit var config: String
    private set
}
