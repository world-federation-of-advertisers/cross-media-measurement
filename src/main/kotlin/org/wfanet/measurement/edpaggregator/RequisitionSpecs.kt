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
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec

/**
 * Whether this [Requisition] belongs to a multimeasurement, which carries its [MeasurementSpec]
 * unsigned and its [RequisitionSpec] in plaintext.
 */
val Requisition.isMultimeasurement: Boolean
  get() = hasUnsignedMeasurementSpec()

/**
 * Returns the [MeasurementSpec] of this [Requisition].
 *
 * A multimeasurement [Requisition] carries it unsigned in `unsigned_measurement_spec`. Any other
 * carries it signed in `measurement_spec`.
 *
 * @throws IllegalArgumentException if this is a multimeasurement [Requisition] that also carries a
 *   signed spec, whose spec is not a multimeasurement, or whose protocol is not TrusTEE v2
 */
fun Requisition.readMeasurementSpec(): MeasurementSpec {
  if (!isMultimeasurement) {
    return measurementSpec.unpack()
  }
  requireTrusTeeV2Multimeasurement()
  return unsignedMeasurementSpec
}

/**
 * Returns the [RequisitionSpec] of this [Requisition].
 *
 * A multimeasurement [Requisition] carries it in plaintext in `requisition_spec`, with no nonce.
 * Any other carries it encrypted to the `DataProvider`'s key in `encrypted_requisition_spec`, which
 * is decrypted with [privateEncryptionKey].
 *
 * @throws IllegalArgumentException if this is a multimeasurement [Requisition] that is not a
 *   well-formed TrusTEE v2 multimeasurement, or that lacks a plaintext spec or has an encrypted one
 * @throws GeneralSecurityException if decryption fails
 */
fun Requisition.readRequisitionSpec(privateEncryptionKey: PrivateKeyHandle): RequisitionSpec {
  if (!isMultimeasurement) {
    return decryptRequisitionSpec(encryptedRequisitionSpec, privateEncryptionKey).unpack()
  }
  requireTrusTeeV2Multimeasurement()
  require(hasRequisitionSpec()) { "$name is a multimeasurement without a requisition_spec" }
  require(!hasEncryptedRequisitionSpec()) {
    "$name sets both requisition_spec and encrypted_requisition_spec"
  }
  return requisitionSpec
}

/**
 * Requires that this multimeasurement [Requisition] is a TrusTEE v2 one with no signed spec, since
 * skipping consent signaling is only sound for that protocol.
 */
private fun Requisition.requireTrusTeeV2Multimeasurement() {
  require(!hasMeasurementSpec()) {
    "$name sets both measurement_spec and unsigned_measurement_spec"
  }
  require(unsignedMeasurementSpec.hasMulti()) {
    "$name has an unsigned_measurement_spec that is not a multimeasurement"
  }
  val protocols = protocolConfig.protocolsList
  require(protocols.isNotEmpty() && protocols.all { it.hasTrusTeeV2() }) {
    "$name has an unsigned_measurement_spec but a protocol other than TrusTEE v2"
  }
}
