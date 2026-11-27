/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.duchy.mill.trustee.v2

import java.nio.ByteBuffer
import java.security.MessageDigest
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceParams
import org.wfanet.measurement.computation.DeterministicTruncatedLaplaceResultNoiser
import org.wfanet.measurement.computation.NoNoise
import org.wfanet.measurement.computation.ResultNoiser
import org.wfanet.measurement.internal.duchy.NoiseMechanism

/**
 * The deterministic noise of the cells of one multimeasurement.
 *
 * Each draw is seeded from [measurementName], the cell's query id and grouping values, and a
 * fingerprint of the data it noises, so that rerunning a computation returns the same figures and
 * two measurements whose cells hold the same data do not share noise. A cell's released figures are
 * seeded by the cell's fingerprint, and each `DataProvider` slice test by that slice's fingerprint
 * alone, so that no `DataProvider`'s vectors change the noise on another's slice test.
 *
 * @param measurementName resource name of the `Measurement` the computation belongs to
 */
class CellNoise(private val params: CellPrivacyParams, private val measurementName: String) {
  /**
   * Whether cells are noised, with deterministic truncated-Laplace noise. [CellPrivacyParams]
   * allows no other mechanism than `NONE`.
   */
  private val noised: Boolean =
    params.noiseMechanism == NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE

  /** Returns the noiser for the released figures of [cell], seeded from its fingerprint. */
  fun cellNoiser(cell: QueryEvaluator.Cell): ResultNoiser = noiser {
    seed(CELL_DOMAIN, cell, cell.fingerprint, dataProvider = null)
  }

  /**
   * Returns the noiser for the slice test of [dataProvider]'s slice of [cell], seeded from that
   * slice's fingerprint.
   */
  fun sliceNoiser(cell: QueryEvaluator.Cell, dataProvider: String): ResultNoiser = noiser {
    seed(SLICE_DOMAIN, cell, cell.sliceFingerprints.getValue(dataProvider), dataProvider)
  }

  /** The mechanism every cell is noised with, as the public API reports it. */
  val publicNoiseMechanism: ProtocolConfig.NoiseMechanism
    get() =
      if (noised) {
        ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
      } else {
        ProtocolConfig.NoiseMechanism.NONE
      }

  /** Variance of the noise on a reach or a frequency bin, each of which a VID moves by 1. */
  val unitVariance: Double
    get() = variance(UNIT_SENSITIVITY)

  /** Variance of the noise on an impression count. */
  val impressionVariance: Double
    get() = if (noised) variance(checkNotNull(params.impressionCap).toDouble()) else 0.0

  private fun noiser(computeSeed: () -> ByteArray): ResultNoiser =
    if (noised) {
      DeterministicTruncatedLaplaceResultNoiser.fromSeed(
        seed = computeSeed(),
        maxFrequencyPerUser = checkNotNull(params.impressionCap),
      )
    } else {
      NoNoise
    }

  /** Returns the variance of a draw of L1 [sensitivity]. */
  private fun variance(sensitivity: Double): Double =
    if (noised) DeterministicTruncatedLaplaceParams.variance(sensitivity) else 0.0

  /**
   * Returns SHA-256 over the length-prefixed seed inputs of [cell]: [domain], [measurementName],
   * the query id, each grouping value, [fingerprint] and, for a slice, [dataProvider].
   */
  private fun seed(
    domain: String,
    cell: QueryEvaluator.Cell,
    fingerprint: ByteArray,
    dataProvider: String?,
  ): ByteArray {
    val digest = MessageDigest.getInstance(SEED_ALGORITHM)
    digest.updatePart(domain.toByteArray(Charsets.UTF_8))
    digest.updatePart(measurementName.toByteArray(Charsets.UTF_8))
    digest.updatePart(cell.queryId.toByteArray(Charsets.UTF_8))
    digest.updateLength(cell.attributeValues.size)
    for (value in cell.attributeValues) {
      // An unset value has its own length, so it is not confused with an empty symbol.
      if (value == null) {
        digest.updateLength(UNSET_LENGTH)
      } else {
        digest.updatePart(value.toByteArray(Charsets.UTF_8))
      }
    }
    digest.updatePart(fingerprint)
    if (dataProvider != null) {
      digest.updatePart(dataProvider.toByteArray(Charsets.UTF_8))
    }
    return digest.digest()
  }

  private fun MessageDigest.updatePart(part: ByteArray) {
    updateLength(part.size)
    update(part)
  }

  private fun MessageDigest.updateLength(length: Int) {
    update(ByteBuffer.allocate(Int.SIZE_BYTES).putInt(length).array())
  }

  companion object {
    private const val SEED_ALGORITHM = "SHA-256"
    private const val CELL_DOMAIN = "cell"
    private const val SLICE_DOMAIN = "slice"
    private const val UNSET_LENGTH = -1
    private const val UNIT_SENSITIVITY = 1.0
  }
}
