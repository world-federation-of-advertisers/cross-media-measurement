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

package org.wfanet.measurement.edpaggregator

import java.security.GeneralSecurityException
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec

/**
 * Reads the [MeasurementSpec] and [RequisitionSpec] of a [Requisition].
 *
 * A multimeasurement [Requisition], one whose `protocol_config.measurement_type` is `MULTI`,
 * carries its [MeasurementSpec] unsigned and its [RequisitionSpec] in plaintext, and is fulfilled
 * by TrusTEE v2, which fulfills nothing else. Any other carries a signed [MeasurementSpec] and an
 * encrypted [RequisitionSpec]. Each reader refuses a [Requisition] that mixes the two forms.
 */
object RequisitionSpecs {
  /** Whether [requisition] belongs to a multimeasurement. */
  fun isMultimeasurement(requisition: Requisition): Boolean =
    requisition.protocolConfig.measurementType == ProtocolConfig.MeasurementType.MULTI

  /**
   * Returns the [MeasurementSpec] of [requisition].
   *
   * @throws IllegalArgumentException if [requisition] mixes the multimeasurement and signed forms
   */
  fun readMeasurementSpec(requisition: Requisition): MeasurementSpec {
    if (!isMultimeasurement(requisition)) {
      requireSignedAndEncrypted(requisition)
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.unpack()
      require(!measurementSpec.hasMulti()) {
        "${requisition.name} has a MultiMeasurementSpec but measurement_type is not MULTI"
      }
      return measurementSpec
    }
    requireMultimeasurement(requisition)
    return requisition.unsignedMeasurementSpec
  }

  /**
   * Returns the [RequisitionSpec] of [requisition], decrypting it with [privateEncryptionKey]
   * unless [requisition] belongs to a multimeasurement.
   *
   * @throws IllegalArgumentException if [requisition] mixes the multimeasurement and signed forms
   * @throws GeneralSecurityException if decryption fails
   */
  fun readRequisitionSpec(
    requisition: Requisition,
    privateEncryptionKey: PrivateKeyHandle,
  ): RequisitionSpec {
    if (!isMultimeasurement(requisition)) {
      requireSignedAndEncrypted(requisition)
      return decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
        .unpack()
    }
    requireMultimeasurement(requisition)
    return requisition.requisitionSpec
  }

  /**
   * Requires that a multimeasurement [requisition] has exactly the multimeasurement form, since
   * skipping consent signaling is only sound for that form.
   */
  private fun requireMultimeasurement(requisition: Requisition) {
    val name = requisition.name
    require(requisition.hasUnsignedMeasurementSpec() && !requisition.hasMeasurementSpec()) {
      "$name is a multimeasurement but does not carry only an unsigned_measurement_spec"
    }
    require(requisition.unsignedMeasurementSpec.hasMulti()) {
      "$name is a multimeasurement but its MeasurementSpec is not a MultiMeasurementSpec"
    }
    require(requisition.hasRequisitionSpec() && !requisition.hasEncryptedRequisitionSpec()) {
      "$name is a multimeasurement but does not carry only a plaintext requisition_spec"
    }
    val protocols = requisition.protocolConfig.protocolsList
    require(protocols.isNotEmpty() && protocols.all { it.hasTrusTeeV2() }) {
      "$name is a multimeasurement but has a protocol other than TrusTEE v2"
    }
  }

  /** Requires that a non-multimeasurement [requisition] carries no multimeasurement field. */
  private fun requireSignedAndEncrypted(requisition: Requisition) {
    require(!requisition.hasUnsignedMeasurementSpec() && !requisition.hasRequisitionSpec()) {
      "${requisition.name} is not a multimeasurement but carries an unsigned or plaintext spec"
    }
    require(requisition.protocolConfig.protocolsList.none { it.hasTrusTeeV2() }) {
      "${requisition.name} is not a multimeasurement but has a TrusTEE v2 protocol"
    }
  }
}
