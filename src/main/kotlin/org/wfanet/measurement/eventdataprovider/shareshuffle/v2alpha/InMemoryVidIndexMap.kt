package org.wfanet.measurement.eventdataprovider.shareshuffle.v2alpha

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import java.nio.ByteOrder
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator
import org.wfanet.measurement.common.toByteString

/**
 * An implementation of [VidIndexMap] that holds the Map in memory.
 *
 * This implementation of [VidIndexMap] creates the mapping from scratch given a [PopulationSpec]
 *
 * Overriding the default hash function can cause incompatibilities between EDPs which can lead to
 * bad measurement. The [hashFunction] is exposed only for testing.
 *
 * @param[populationSpec] The [PopulationSpec] to build the map for.
 * @param [hashFunction] The hash function to use for hashing VIDs.
 * @constructor Creates a [VidIndexMap] for the given [PopulationSpec]
 * @throws [PopulationSpecValidationException] if the [populationSpec] is invalid
 */
class InMemoryVidIndexMap(
  override val populationSpec: PopulationSpec,
  private val hashFunction: (Long, ByteString) -> Long = Companion::hashVidToLongWithFarmHash,
) : VidIndexMap {
  // TODO(@kungfucraig): Provide a constructor that reads the vid->index map from a file.

  override val size
    get() = indexMap.size.toLong()

  /** A map of a VID to its index in the [Frequency Vector]. */
  private val indexMap = hashMapOf<Long, Int>()

  /**
   * A salt value to ensure the output of the hash used by the VidIndexMap is different from other
   * functions that hash VIDs (e.g. the labeler). These are the first several digits of phi (the
   * golden ratio) added to the date this value was created.
   */
  private val salt = (1_618_033L + 20_240_417L).toByteString(ByteOrder.BIG_ENDIAN)

  /** A data class for a VID and its hash value. */
  data class VidAndHash(val vid: Long, val hash: Long) : Comparable<VidAndHash> {
    override operator fun compareTo(other: VidAndHash): Int =
      compareValuesBy(this, other, { it.hash }, { it.vid })
  }

  init {
    PopulationSpecValidator.validateVidRangesList(populationSpec).getOrThrow()

    val hashes = mutableListOf<VidAndHash>()
    for (subPop in populationSpec.subpopulationsList) {
      for (range in subPop.vidRangesList) {
        for (vid in range.startVid..range.endVidInclusive) {
          hashes.add(VidAndHash(vid, hashFunction(vid, salt)))
        }
      }
    }

    hashes.sortWith(compareBy<VidAndHash>() { it })

    for ((index, vidAndHash) in hashes.withIndex()) {
      indexMap[vidAndHash.vid] = index
    }
  }

  /**
   * Returns the index in the [FrequencyVector] for the given [vid].
   *
   * @throws VidNotFoundException if the [vid] does not exist in the map
   */
  override operator fun get(vid: Long): Int = indexMap[vid] ?: throw VidNotFoundException(vid)

  companion object {
    /**
     * Hash a VID with FarmHash and return the output as a [Long]
     *
     * The input of the hash function is determined by converting the [vid] to a byte array with big
     * endian ordering and concatenating the [salt] to it.
     *
     * This input is passed to farmHashFingerprint64() whose output is a byte array.
     *
     * The bytearray is converted to a long in little endian order, which is then returned.
     *
     * @param [vid] the vid to hash
     * @param [salt] the value concatenated to the [vid] prior to hashing
     * @returns the hash of the vid
     */
    fun hashVidToLongWithFarmHash(vid: Long, salt: ByteString): Long {
      val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
      return Hashing.farmHashFingerprint64().hashBytes(hashInput.toByteArray()).asLong()
    }
  }
}
