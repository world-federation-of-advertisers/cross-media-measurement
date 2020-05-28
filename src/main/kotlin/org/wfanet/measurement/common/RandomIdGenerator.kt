package org.wfanet.measurement.common

/**
 * Interface for ID generation.
 */
interface RandomIdGenerator {
  /**
   * Generates a random positive 64-bit integer.
   */
  fun generate(): Long
}
