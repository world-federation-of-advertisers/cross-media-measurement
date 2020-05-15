package org.wfanet.measurement.common

/**
 * The role the duchy is playing in an ongoing computation.
 */
enum class DuchyRole {
  /**
   * The duchy is the first duchy in the ring of MPC workers. They are
   * expected to do a few more steps that other duchies, such as homomorphic
   * operations on ciphertexts.
   */
  PRIMARY,

  /**
   * The duchy is not the first duchy in the ring.
   */
  SECONDARY;
}
