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

package org.wfanet.measurement.duchy

import java.math.BigInteger
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.crypto.ElGamalPublicKey
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.DuchyPublicKeyConfig
import picocli.CommandLine

private typealias ConfigMapEntry = Map.Entry<String, DuchyPublicKeyConfig.Entry>

/** Bytes per elliptic curve point. */
private const val BYTES_PER_EC_POINT = 33

/** The size of a [generator]. */
private val GENERATOR_SIZE = BYTES_PER_EC_POINT

/** The size of an [element]. */
private val ELEMENT_SIZE = BYTES_PER_EC_POINT

/** Map of Duchy name to public key. */
typealias DuchyPublicKeyMap = Map<String, ElGamalPublicKey>

class DuchyPublicKeys(configMessage: DuchyPublicKeyConfig) {
  /** Map of CombinedPublicKey resource ID to [Entry]. */
  private val entries: Map<String, Entry>
  init {
    require(configMessage.entriesCount > 0) { "Duchy public key config has no entries" }
    entries = configMessage.entriesMap.mapValues { it.toDuchyPublicKeysEntry() }
  }

  /** The latest (most recent) entry. */
  val latest: Entry by lazy {
    entries.maxBy { it.value.combinedPublicKeyVersion }!!.value
  }

  /** Returns the [Entry] for the specified CombinedPublicKey resource ID. */
  fun get(combinedPublicKeyId: String): Entry? = entries[combinedPublicKeyId]

  data class Entry(
    private val publicKeyMap: DuchyPublicKeyMap,
    val combinedPublicKeyId: String,
    val combinedPublicKey: ElGamalPublicKey,
    val combinedPublicKeyVersion: Long,
    val curveId: Int
  ) : DuchyPublicKeyMap by publicKeyMap

  class Flags {
    @CommandLine.Option(
      names = ["--duchy-public-keys-config"],
      description = ["DuchyPublicKeyConfig proto message in text format."],
      required = true
    )
    lateinit var config: String
      private set
  }

  companion object {
    /** Constructs a [DuchyPublicKeys] instance from command-line flags. */
    fun fromFlags(flags: Flags): DuchyPublicKeys {
      val configMessage = flags.config.reader().use {
        parseTextProto(it, DuchyPublicKeyConfig.getDefaultInstance())
      }
      return DuchyPublicKeys(configMessage)
    }
  }
}

private fun ConfigMapEntry.toDuchyPublicKeysEntry(): DuchyPublicKeys.Entry {
  with(value) {
    require(elGamalGenerator.size() == GENERATOR_SIZE) {
      "Expected $GENERATOR_SIZE bytes for generator. Got ${elGamalGenerator.size()}."
    }
    require(combinedElGamalElement.size() == ELEMENT_SIZE) {
      "Expected $ELEMENT_SIZE bytes for element. Got ${combinedElGamalElement.size()}."
    }
    return DuchyPublicKeys.Entry(
      publicKeyMap = elGamalElementsMap.mapValues {
        require(it.value.size() == ELEMENT_SIZE) {
          "Expected $ELEMENT_SIZE bytes for element. Got ${it.value.size()}."
        }
        ElGamalPublicKey.newBuilder().apply {
          generator = elGamalGenerator
          element = it.value
        }.build()
      },
      combinedPublicKeyId = key,
      combinedPublicKey = ElGamalPublicKey.newBuilder().apply {
        generator = elGamalGenerator
        element = combinedElGamalElement
      }.build(),
      combinedPublicKeyVersion = combinedPublicKeyVersion,
      curveId = ellipticCurveId
    )
  }
}

fun DuchyPublicKeyMap.toDuchyOrder(): DuchyOrder {
  return DuchyOrder(map { Duchy(it.key, BigInteger(it.value.toByteArray())) }.toSet())
}
