package org.wfanet.measurement.common.identity

import picocli.CommandLine

/**
 * Global singleton listing all the valid duchy ids, configurable by a flag.
 */
object DuchyIds {
  lateinit var ALL: Set<String>
    private set

  val size: Int
    get() = ALL.size

  fun setDuchyIdsFromFlags(duchyIdFlags: DuchyIdFlags) {
    require(!DuchyIds::ALL.isInitialized)
    require(duchyIdFlags.duchyIds.isNotEmpty())
    ALL = duchyIdFlags.duchyIds.toSet()
  }

  fun setDuchyIdsForTest(duchyIds: Set<String>) {
    ALL = duchyIds
  }
}

/**
 * Flag for setting global [DuchyIds.ALL].
 *
 * Usage:
 *   fun run(@CommandLine.Mixin duchyIdFlags: DuchyIdFlags, ...) {
 *     DuchyIds.setDuchyIdsFromFlags(duchyIdFlags)
 *     ...
 *   }
 */
class DuchyIdFlags {
  @CommandLine.Option(
    names = ["--duchy-ids"],
    description = ["List of all valid Duchy ids"],
    split = ",",
    required = true
  )
  lateinit var duchyIds: Array<String>
    private set
}
