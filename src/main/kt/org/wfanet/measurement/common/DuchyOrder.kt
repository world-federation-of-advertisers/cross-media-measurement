package org.wfanet.measurement.common

import java.math.BigInteger
import java.security.MessageDigest

data class Duchy(val name: String, val publicKey: BigInteger)

/**
 * Determines ordering of duchies for computations.
 */
class DuchyOrder(nodes: Set<Duchy>) {
  private val orderedNodes = nodes.sortedBy { it.publicKey }.map {it.name}
  private val numberOfNodes = BigInteger.valueOf(orderedNodes.size.toLong())

  /**
   * Returns the ordering of [Duchy.name] for a computation based on the identifier.
   *
   * The first item in the list is the primary node for the computation. Each [Duchy] sends
   * messages to the next node in the list. The list is implicitly a ring, so the last [Duchy]
   * sends its results to the first.
   */
  fun computationOrder(computationId: Long): List<String> {
    val primaryIndex = sha1Mod(computationId, numberOfNodes)

    return orderedNodes.subList(primaryIndex, orderedNodes.size) +
           orderedNodes.subList(0, primaryIndex)
  }
}

/** Returns sha1(value) % n. */
fun sha1Mod(value: Long, n: BigInteger): Int {
  val md = MessageDigest.getInstance("SHA1")
  val hash = BigInteger(md.digest(value.toBigInteger().toByteArray()))
  return hash.mod(n).longValueExact().toInt()
}
