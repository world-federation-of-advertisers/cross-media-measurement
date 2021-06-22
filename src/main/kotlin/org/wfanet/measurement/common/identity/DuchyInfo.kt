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

package org.wfanet.measurement.common.identity

import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.DuchyRpcConfig
import picocli.CommandLine

object DuchyInfo {
  lateinit var entries: Array<Entry>
  val count: Int
    get() = DuchyInfo.entries.size
  val ALL_DUCHY_IDS: Set<String>
    get() = DuchyInfo.entries.map { it.duchyId }.toSet()

  fun initializeFromFlags(flags: DuchyInfoFlags) {
    require(!DuchyInfo::entries.isInitialized)
    val configMessage =
      flags.config.reader().use { parseTextProto(it, DuchyRpcConfig.getDefaultInstance()) }
    require(configMessage.duchiesCount > 0) { "Duchy info config has no entries" }
    entries = configMessage.duchiesList.map { it.toDuchyInfoEntry() }.toTypedArray()
  }

  /** Returns the [Entry] for the specified root cert key ID. */
  fun getByRootCertificateSkid(rootCertificateSkid: String): Entry? {
    return entries.firstOrNull { it.rootCertificateSkid == rootCertificateSkid }
  }

  /** Returns the [Entry] for the specified Duchy ID. */
  fun getByDuchyId(duchyId: String): Entry? {
    return entries.firstOrNull { it.duchyId == duchyId }
  }

  fun setForTest(duchyIds: Set<String>) {
    entries = duchyIds.map { DuchyInfo.Entry(it, "hostname-$it", "cert-id-$it") }.toTypedArray()
  }

  data class Entry(
    val duchyId: String,
    val computationControlServiceTarget: String,
    val rootCertificateSkid: String
  )
}

class DuchyInfoFlags {
  @CommandLine.Option(
    names = ["--duchy-info-config"],
    description = ["DuchyRpcConfig proto message in text format."],
    required = true
  )
  lateinit var config: String
    private set
}

private fun DuchyRpcConfig.Duchy.toDuchyInfoEntry(): DuchyInfo.Entry {
  return DuchyInfo.Entry(duchyId, computationControlServiceTarget, rootCertificateSkid)
}
