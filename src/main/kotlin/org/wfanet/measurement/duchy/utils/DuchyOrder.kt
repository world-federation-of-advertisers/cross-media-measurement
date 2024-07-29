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

package org.wfanet.measurement.duchy.utils

import java.math.BigInteger
import java.security.MessageDigest

data class Duchy(val name: String, val publicKey: String)

/**
 * Order the duchies by their public keys and the globalComputationId.
 *
 * @param nodes the duchies that participant in the ordering. For LLV2, nodes contain
 *   non-aggregators.
 * @param globalComputationId the id of the computation this order is used for.
 * @return the list of ordered duchy names.
 */
fun getDuchyOrderByPublicKeysAndComputationId(
  nodes: Set<Duchy>,
  globalComputationId: String,
): List<String> {
  return nodes.sortedBy { sha1Hash(it.publicKey + globalComputationId) }.map { it.name }
}

/**
 * Gets the next duchy in the ring.
 *
 * @param duchyOrder the ordered duchy list.
 * @param currentDuchy the id of the current duchy.
 * @return the id of the next duchy in the ring. If the current duchy is the last one in the list,
 *   the first duchy will be returned.
 */
fun getNextDuchy(duchyOrder: List<String>, currentDuchy: String): String {
  require(duchyOrder.contains(currentDuchy)) { "$currentDuchy is not in the $duchyOrder" }
  return duchyOrder[(duchyOrder.indexOf(currentDuchy) + 1) % duchyOrder.size]
}

/**
 * Gets all the following duchies in the ring.
 *
 * @param duchyOrder the ordered duchy list.
 * @param currentDuchy the id of the current duchy.
 * @return the id of the following duchy in the ring. If the current duchy is the last one in the
 *   list, an empty list is returned.
 */
fun getFollowingDuchies(duchyOrder: List<String>, currentDuchy: String): List<String> {
  require(duchyOrder.contains(currentDuchy)) { "$currentDuchy is not in the $duchyOrder" }
  return duchyOrder.subList(duchyOrder.indexOf(currentDuchy) + 1, duchyOrder.size)
}

/** Gets the SHA1 hash of the [value] as a [BigInteger] */
fun sha1Hash(value: String): BigInteger {
  val md = MessageDigest.getInstance("SHA1")
  return BigInteger(md.digest(value.toByteArray()))
}
