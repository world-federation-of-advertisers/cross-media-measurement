// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider.daemon

import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.dataprovider.common.EncryptedSketchGenerator
import org.wfanet.measurement.dataprovider.common.RequisitionDecoder
import org.wfanet.measurement.dataprovider.common.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.common.UnfulfilledRequisitionProvider

/**
 * Combines the constructor parameters together into a workflow to find and fulfill a [Requisition].
 *
 * @property unfulfilledRequisitionProvider helper to fetch unfulfilled [Requisition]s
 * @property requisitionDecoder helper for cryptographic operations on [Requisition]s
 * @property requisitionFulfiller helper to upload encrypted sketches
 * @property sketchGenerator helper to build encrypted sketches
 */
class RequisitionFulfillmentWorkflow(
  private val unfulfilledRequisitionProvider: UnfulfilledRequisitionProvider,
  private val requisitionDecoder: RequisitionDecoder,
  private val sketchGenerator: EncryptedSketchGenerator,
  private val requisitionFulfiller: RequisitionFulfiller
) {
  suspend fun execute() {
    val requisition: Requisition = unfulfilledRequisitionProvider.get() ?: return

    val measurementSpec = requisitionDecoder.decodeMeasurementSpec(requisition)
    val requisitionSpec = requisitionDecoder.decodeRequisitionSpec(requisition)

    val sketchChunks: Flow<ByteString> = when (measurementSpec.forCase) {
      MeasurementSpec.ForCase.ENCRYPTED_SKETCH ->
        sketchGenerator.generate(requisitionSpec, measurementSpec.encryptedSketch)
      else ->
        throw IllegalArgumentException("Case ${measurementSpec.forCase} unsupported.")
    }

    requisitionFulfiller.fulfillRequisition(requisition.key, sketchChunks)
  }
}
