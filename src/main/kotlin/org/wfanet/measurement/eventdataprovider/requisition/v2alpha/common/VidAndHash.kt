package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

/** A data class for a VID and its hash value. */
data class VidAndHash(val vid: Int, val hash: Long) : Comparable<VidAndHash> {
  override operator fun compareTo(other: VidAndHash): Int =
    compareValuesBy(this, other, { it.hash }, { it.vid })
}
