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

package org.wfanet.measurement.dataprovider.fake

import com.google.protobuf.ByteString
import java.util.logging.Logger
import kotlinx.coroutines.yield
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedData
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.dataprovider.common.ElGamalPublicKeyStore
import org.wfanet.measurement.dataprovider.common.EncryptedSketchGenerator
import org.wfanet.measurement.dataprovider.common.RequisitionFulfiller
import org.wfanet.measurement.dataprovider.common.SketchConfigStore
import org.wfanet.measurement.dataprovider.common.UnfulfilledRequisitionProvider

/** Size of chunks in each streamed message for uploading sketches. */
private const val DEFAULT_STREAM_BYTE_BUFFER_SIZE: Int = 1024 * 32 // 32 KiB

/**
 * Implements a basic Data Provider that listens for [Requisition]s and fulfills them.
 */
class FakeDataProvider(
  private val sketchGenerator: EncryptedSketchGenerator,
  private val unfulfilledRequisitionProvider: UnfulfilledRequisitionProvider,
  private val requisitionFulfiller: RequisitionFulfiller,
  private val publicKeyStore: ElGamalPublicKeyStore,
  private val sketchConfigStore: SketchConfigStore,
  private val throttler: Throttler,
  private val streamByteBufferSize: Int = DEFAULT_STREAM_BYTE_BUFFER_SIZE
) {
  suspend fun start() {
    while (true) {
      yield()
      logAndSuppressExceptionSuspend {
        throttler.onReady {
          logger.info("Polling for a Requisition")
          val requisition: Requisition? = unfulfilledRequisitionProvider.get()
          if (requisition != null) {
            logger.info("Found Requisition ${requisition.key}")
            fulfillRequisition(requisition)
          }
        }
      }
    }
  }

  private suspend fun fulfillRequisition(requisition: Requisition) {
    logger.info("Fulfilling Requisition ${requisition.key}")

    val measurementSpec = getMeasurementSpec(requisition)

    check(measurementSpec.forCase == MeasurementSpec.ForCase.ENCRYPTED_SKETCH) {
      "Other types of measurements are not yet supported by FakeDataProvider"
    }
    val encryptedSketchConfig = measurementSpec.encryptedSketch

    val requisitionSpec = decryptRequisitionSpec(requisition)
    val publicKey = publicKeyStore.get(encryptedSketchConfig.combinedPublicKey)
    val sketchConfig = sketchConfigStore.get(encryptedSketchConfig.sketchConfig)
    val sketch: ByteString = sketchGenerator.generate(requisitionSpec, publicKey, sketchConfig)

    requisitionFulfiller.fulfillRequisition(
      requisition.key,
      sketch.asBufferedFlow(streamByteBufferSize)
    )
  }

  private fun getMeasurementSpec(requisition: Requisition): MeasurementSpec {
    val serializedMeasurementSpec = requisition.measurementSpec

    // TODO: validate signature on `serializedMeasurementSpec`.
    return MeasurementSpec.parseFrom(serializedMeasurementSpec.data)
  }

  private fun decryptRequisitionSpec(requisition: Requisition): RequisitionSpec {
    // TODO: this should decrypt instead of just deserializing.
    val signedData = SignedData.parseFrom(requisition.encryptedRequisitionSpec)
    // TODO: this should validate the signature first.
    return RequisitionSpec.parseFrom(signedData.data)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
