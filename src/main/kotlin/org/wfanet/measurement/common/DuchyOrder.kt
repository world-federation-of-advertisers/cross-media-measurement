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

package org.wfanet.measurement.common

import java.math.BigInteger
import java.security.MessageDigest

data class Duchy(val name: String, val publicKey: BigInteger)

/**
 * Determines ordering of duchies for computations.
 */
class DuchyOrder(nodes: Set<Duchy>) {
  private val orderedNodes = nodes.sortedBy { it.publicKey }.map { it.name }
  private val numberOfNodes = BigInteger.valueOf(orderedNodes.size.toLong())

  /**
   * Returns the ordering of [Duchy.name] for a computation based on the identifier.
   *
   * The first item in the list is the primary node for the computation. Each [Duchy] sends
   * messages to the next node in the list. The list is implicitly a ring, so the last [Duchy]
   * sends its results to the first.
   */
  fun computationOrder(globalComputationId: String): List<String> {
    val primaryIndex = sha1Mod(globalComputationId, numberOfNodes)

    return orderedNodes.subList(primaryIndex, orderedNodes.size) +
      orderedNodes.subList(0, primaryIndex)
  }

  /**
   * Returns the [DuchyPosition] for a duchy name in a computation.
   */
  fun positionFor(globalComputationId: String, name: String): DuchyPosition {
    val ordered = computationOrder(globalComputationId)
    val indexOfThisDuchy = ordered.indexOf(name)

    return DuchyPosition(
      role = when {
        indexOfThisDuchy < 0 -> error("Duchy not in computation?")
        indexOfThisDuchy == 0 -> DuchyRole.PRIMARY
        else -> DuchyRole.SECONDARY
      },
      prev = ordered.wrapAroundGet(indexOfThisDuchy - 1),
      next = ordered.wrapAroundGet(indexOfThisDuchy + 1),
      primary = ordered[0]
    )
  }
}

data class DuchyPosition(
  val role: DuchyRole,
  val prev: String,
  val next: String,
  val primary: String
)

/** Gets the index % size item of list. */
private fun List<String>.wrapAroundGet(index: Int): String {
  val i = (index + size) % size
  return this[i]
}

/** Returns sha1(value) % n. */
fun sha1Mod(value: String, n: BigInteger): Int {
  val md = MessageDigest.getInstance("SHA1")
  val hash = BigInteger(md.digest(value.toByteArray()))
  return hash.mod(n).longValueExact().toInt()
}
