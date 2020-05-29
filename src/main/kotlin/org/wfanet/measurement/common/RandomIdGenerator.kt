package org.wfanet.measurement.common

/**
 * Interface for ID generation.
 */
interface RandomIdGenerator {
  /**
   * Generates a random internal id.
   */
  fun generateInternalId(): InternalId

  /**
   * Generates a random external id.
   */
  fun generateExternalId(): ExternalId
}
