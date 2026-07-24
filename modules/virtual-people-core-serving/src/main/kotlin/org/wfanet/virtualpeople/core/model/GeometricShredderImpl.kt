// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.model

import com.google.common.hash.Hashing
import com.google.protobuf.Descriptors.FieldDescriptor
import java.nio.charset.StandardCharsets
import kotlin.math.floor
import kotlin.math.ln
import org.wfanet.virtualpeople.common.GeometricShredder
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.fieldfilter.utils.getFieldFromProto
import org.wfanet.virtualpeople.common.fieldfilter.utils.getValueFromProto
import org.wfanet.virtualpeople.common.fieldfilter.utils.setValueToProtoBuilder
import org.wfanet.virtualpeople.core.model.utils.expHash

/**
 * @param psi The shredding probability parameter psi, which corresponds to the success probability
 *   parameter of geometric distribution as p = 1 − psi.
 * @param randomnessField The descriptors of the field in LabelerEvent, which provides the
 *   randomness for the geometric shredding.
 * @param targetField The descriptors of the field in LabelerEvent, which is to be updated by the
 *   shred value.
 * @param randomSeed The seed used to generate the shred hash output.
 */
internal class GeometricShredderImpl
private constructor(
  private val psi: Float,
  private val randomnessField: List<FieldDescriptor>,
  private val targetField: List<FieldDescriptor>,
  private val randomSeed: String,
) : AttributesUpdaterInterface {

  /** Compute the shred hash. */
  private fun shredHash(event: LabelerEvent.Builder): ULong {
    /** No shredding. */
    if (psi == 0.0f) {
      return 0UL
    }

    val randomnessFieldValue = getValueFromProto<ULong>(event, randomnessField)
    if (!randomnessFieldValue.isSet) {
      error("The randomness field is not set in the event.")
    }
    val randomnessValue = randomnessFieldValue.value

    /** Certain shredding if randomness_value is not 0. */
    if (psi == 1.0f) {
      return randomnessValue
    }

    /** shred_hash = Floor(ExpHash(randomness_value) / (- Log(psi))) */
    val expHashValue = expHash(randomnessValue.toString())
    return floor(expHashValue / (-ln(psi))).toULong()
  }

  /**
   * Updates the field referred by [targetField] in [event] with the shred value.
   *
   * Throws an error if [randomnessField] or [targetField] is not set.
   */
  override fun update(event: LabelerEvent.Builder) {
    val shredHash = shredHash(event)
    if (shredHash == 0UL) {
      return
    }

    val targetFieldValue = getValueFromProto<ULong>(event, targetField)
    if (!targetFieldValue.isSet) {
      error("The target field is not set in the event.")
    }

    val targetValue = targetFieldValue.value
    val fullSeed = "$targetValue-shred-$shredHash-$randomSeed"
    val shred =
      Hashing.farmHashFingerprint64()
        .hashString(fullSeed, StandardCharsets.UTF_8)
        .asLong()
        .toULong()
    setValueToProtoBuilder(event, targetField, shred)
  }

  internal companion object {

    /**
     * Always use [AttributesUpdaterInterface].build to get an [AttributesUpdaterInterface] object.
     * Users should not call the factory method or the constructor of the derived classes directly.
     *
     * Throws an error when any of the following happens:
     * 1. [config].psi is not in [0, 1].
     * 2. [config].randomnessField does not refer to a valid field.
     * 3. [config].randomnessField does not refer to a uint64 field.
     * 4. [config].targetField does not refer to a valid field.
     * 5. [config].targetField does not refer to a uint64 field.
     */
    internal fun build(config: GeometricShredder): GeometricShredderImpl {
      val psi = config.psi
      if (psi < 0 || psi > 1) {
        error("Psi is not in [0, 1] in GeometricShredder: $config")
      }

      /** getFieldFromProto throws error if it does not refer to a valid field. */
      val randomnessField = getFieldFromProto(LabelerEvent.getDescriptor(), config.randomnessField)
      if (randomnessField.last().type != FieldDescriptor.Type.UINT64) {
        error("randomness_field type is not uint64 in GeometricShredder: $config")
      }

      /** getFieldFromProto throws error if it does not refer to a valid field. */
      val targetField = getFieldFromProto(LabelerEvent.getDescriptor(), config.targetField)
      if (targetField.last().type != FieldDescriptor.Type.UINT64) {
        error("target_field type is not uint64 in GeometricShredder: $config")
      }

      return GeometricShredderImpl(psi, randomnessField, targetField, config.randomSeed)
    }
  }
}
