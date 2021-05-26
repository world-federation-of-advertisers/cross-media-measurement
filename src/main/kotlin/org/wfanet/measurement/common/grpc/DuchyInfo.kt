
// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.grpc
import com.google.protobuf.TextFormat
import picocli.CommandLine
import org.wfanet.measurement.config.DuchyInfoConfig


class DuchyInfo(configMessage: DuchyInfoConfig) {
  /** Map of CombinedPublicKey resource ID to [Entry]. */
  private val entries: Array<Entry>

  init {
    require(configMessage.duchiesCount > 0) { "Duchy info config has no entries" }
    entries = configMessage.duchiesList.map{ it.toDuchyInfoEntry() }.toTypedArray()
  }

  /** The latest (most recent) entry. */
  val latest: Entry by lazy {
//    entries.maxBy { it.value.combinedPublicKeyVersion }!!.value
    entries[0]
  }


  /** Returns the [Entry] for the specified root cert key ID. */
  fun get(keyId: String): Entry? {
    entries.forEach {
      if (it.rootCertId == keyId) {
        return it
      }
    }
    return null
  }



  data class Entry(
    val duchyId: String,
    val hostName: String,
    val rootCertId: String
  ) //: DuchyPublicKeyMap by publicKeyMap

  class Flags {
    @CommandLine.Option(
      names = ["--duchy-info-config"],
      description = ["DuchyInfoConfig proto message in text format."],
      required = true
    )
    lateinit var config: String
      private set
  }

  companion object {
    fun fromFlags(flags: Flags): DuchyInfo {
      val configMessage = TextFormat.parse(flags.config, DuchyInfoConfig::class.java)
      return DuchyInfo(configMessage)
    }
  }
}


private fun DuchyInfoConfig.Duchy.toDuchyInfoEntry(): DuchyInfo.Entry {
  return DuchyInfo.Entry(
    getDuchyId(),
    getHostName(),
    getRootCertId()
  )
}

/*
private fun DuchyPublicKeyConfig.Entry.toDuchyPublicKeysEntry(): DuchyPublicKeys.Entry {
  return DuchyPublicKeys.Entry(
    elGamalElementsMap.mapValues {
      ElGamalPublicKey(ellipticCurveId, elGamalGenerator, it.value)
    },
    ElGamalPublicKey(ellipticCurveId, elGamalGenerator, combinedElGamalElement),
    combinedPublicKeyVersion
  )
}

fun DuchyPublicKeyMap.toDuchyOrder(): DuchyOrder {
  return DuchyOrder(map { Duchy(it.key, BigInteger(it.value.toByteArray())) }.toSet())
}
*/
