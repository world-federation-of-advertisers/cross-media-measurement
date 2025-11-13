/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers

import io.grpc.StatusException
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

/**
 * Fulfiller for direct measurements.
 *
 * @param requisitionName The name of the requisition to fulfill.
 * @param dataProviderCertificateName The name of the data provider certificate.
 * @param measurementResult The measurement result to fulfill the requisition with.
 * @param nonce The nonce to use for the fulfillment.
 * @param measurementEncryptionPublicKey The encryption public key to use for the fulfillment.
 * @param directProtocolConfig The direct protocol configuration.
 * @param directNoiseMechanism The direct noise mechanism to use.
 * @param requisitionsStub The stub for the Requisitions service.
 * @param dataProviderSigningKeyHandle The signing key handle for the data provider.
 * @param dataProviderCertificateKey The certificate key for the data provider.
 */
class DirectMeasurementFulfiller(
  private val requisitionName: String,
  private val requisitionDataProviderCertificateName: String,
  private val measurementResult: Measurement.Result,
  private val requisitionNonce: Long,
  private val measurementEncryptionPublicKey: EncryptionPublicKey,
  private val directProtocolConfig: ProtocolConfig.Direct,
  private val directNoiseMechanism: DirectNoiseMechanism,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val requisitionsStub: RequisitionsCoroutineStub,
) : MeasurementFulfiller {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

  /** Fulfills a requisition. */
  override suspend fun fulfillRequisition() {
    logger.log(Level.INFO, "Direct MeasurementResult: $measurementResult")

    DataProviderCertificateKey.fromName(requisitionDataProviderCertificateName)
      ?: throw Exception("Invalid data provider certificate")

    val signedResult: SignedMessage = signResult(measurementResult, dataProviderSigningKeyHandle)
    val signedEncryptedResult: EncryptedMessage =
      encryptResult(signedResult, measurementEncryptionPublicKey)

    try {
      val requisition =
        requisitionsStub.getRequisition(getRequisitionRequest { name = requisitionName })
      if (requisition.state === Requisition.State.UNFULFILLED) {
        requisitionsStub.fulfillDirectRequisition(
          fulfillDirectRequisitionRequest {
            name = requisitionName
            encryptedResult = signedEncryptedResult
            nonce = requisitionNonce
            certificate = dataProviderCertificateKey.toName()
            etag = requisition.etag
          }
        )
      } else {
        logger.info("Cannot fulfill requisition $requisitionName with state ${requisition.state}")
      }
    } catch (e: StatusException) {
      throw Exception("Error fulfilling direct requisition $requisitionName", e)
    }
  }
}
