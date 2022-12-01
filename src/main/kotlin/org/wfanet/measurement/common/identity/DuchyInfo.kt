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

import com.google.protobuf.ByteString
import java.io.File
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.DuchyCertConfig
import picocli.CommandLine

object DuchyInfo {
  lateinit var entries: Map<String, Entry>
  val ALL_DUCHY_IDS: Set<String>
    get() = entries.keys

  fun initializeFromFlags(flags: DuchyInfoFlags) {
    require(!DuchyInfo::entries.isInitialized)
    val configMessage =
      flags.config.reader().use { parseTextProto(it, DuchyCertConfig.getDefaultInstance()) }
    initializeFromConfig(configMessage)
  }

  fun initializeFromConfig(certConfig: DuchyCertConfig) {
    require(!DuchyInfo::entries.isInitialized)
    require(certConfig.duchiesCount > 0) { "Duchy info config has no entries" }
    entries =
      certConfig.duchiesList.associateBy(DuchyCertConfig.Duchy::getDuchyId) {
        it.toDuchyInfoEntry()
      }
  }

  /** Returns the [Entry] for the specified root cert key ID. */
  fun getByRootCertificateSkid(rootCertificateSkid: ByteString): Entry? {
    return entries.values.firstOrNull { it.rootCertificateSkid == rootCertificateSkid }
  }

  /** Returns the [Entry] for the specified Duchy ID. */
  fun getByDuchyId(duchyId: String): Entry? {
    return entries.values.firstOrNull { it.duchyId == duchyId }
  }

  fun setForTest(duchyIds: Set<String>) {
    entries =
      duchyIds.associateWith { Entry(it, "cert-host-$it", ByteString.copyFromUtf8("cert-id-$it")) }
  }

  data class Entry(
    val duchyId: String,
    val computationControlServiceCertHost: String,
    val rootCertificateSkid: ByteString
  )
}

class DuchyInfoFlags {
  @CommandLine.Option(
    names = ["--duchy-info-config"],
    description = ["DuchyCertConfig proto message in text format."],
    required = true
  )
  lateinit var config: File
    private set
}

private fun DuchyCertConfig.Duchy.toDuchyInfoEntry(): DuchyInfo.Entry {
  return DuchyInfo.Entry(duchyId, computationControlServiceCertHost, rootCertificateSkid)
}
