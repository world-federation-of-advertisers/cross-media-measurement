package org.wfanet.measurement.common.testing

import kotlin.random.Random

fun generateIndependentSets(
  universeSize: Long,
  setSize: Int,
  numSets: Int,
  random: Random = Random
) = iterator {
  require(setSize > 0 && setSize <= universeSize) {
    "Each set size must be between 1 and universe size!"
  }
  require(numSets > 0) { "Number of sets must be greater than 0!" }
  repeat(numSets) {
    // Use Robert Floyd algorithm for sampling without replacement
    val set = mutableSetOf<Long>()
    for (j in universeSize - setSize until universeSize) {
      val t = random.nextLong(j + 1)
      if (set.contains(t)) {
        set.add(j)
      } else {
        set.add(t)
      }
    }
    yield(set.toList())
  }
}
