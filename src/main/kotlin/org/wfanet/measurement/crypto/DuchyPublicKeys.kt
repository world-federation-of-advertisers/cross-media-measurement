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

package org.wfanet.measurement.crypto

import com.google.protobuf.TextFormat
import java.io.File
import java.math.BigInteger
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.config.DuchyPublicKeyConfig
import picocli.CommandLine

/** Map of Duchy name to public key. */
typealias DuchyPublicKeyMap = Map<String, ElGamalPublicKey>

class DuchyPublicKeys(configMessage: DuchyPublicKeyConfig) {
  /** Map of CombinedPublicKey resource ID to [Entry]. */
  private val entries: Map<String, Entry>
  init {
    require(configMessage.entriesCount > 0) { "Duchy public key config has no entries" }
    entries = configMessage.entriesMap.mapValues { it.value.toDuchyPublicKeysEntry() }
  }

  /** The latest (most recent) entry. */
  val latest: Entry by lazy {
    entries.maxBy { it.value.combinedPublicKeyVersion }!!.value
  }

  /** Returns the [Entry] for the specified CombinedPublicKey resource ID. */
  fun get(combinedPublicKeyId: String): Entry? = entries[combinedPublicKeyId]

  data class Entry(
    private val publicKeyMap: DuchyPublicKeyMap,
    val combinedPublicKey: ElGamalPublicKey,
    val combinedPublicKeyVersion: Long
  ) : DuchyPublicKeyMap by publicKeyMap

  class Flags {
    @CommandLine.Option(
      names = ["--duchy-public-keys-config-file"],
      description = ["File path for DuchyPublicKeyConfig proto message in text format."],
      required = true
    )
    lateinit var configFile: File
      private set
  }

  companion object {
    /** Constructs a [DuchyPublicKeys] instance from command-line flags. */
    fun fromFlags(flags: Flags): DuchyPublicKeys {
      val configMessage = TextFormat.parse(
        flags.configFile.readText(),
        DuchyPublicKeyConfig::class.java
      )
      return DuchyPublicKeys(configMessage)
    }
  }
}

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
