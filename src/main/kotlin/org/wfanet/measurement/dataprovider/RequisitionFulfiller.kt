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

package org.wfanet.measurement.dataprovider

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.unpack
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.common.NonceMismatchException
import org.wfanet.measurement.consent.client.common.PublicKeyMismatchException
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.consent.client.dataprovider.verifyMeasurementSpec
import org.wfanet.measurement.consent.client.dataprovider.verifyRequisitionSpec

data class DataProviderData(
  /** The DataProvider's public API resource name. */
  val name: String,
  /** The DataProvider's decryption key. */
  val privateEncryptionKey: PrivateKeyHandle,
  /** The DataProvider's consent signaling signing key. */
  val signingKeyHandle: SigningKeyHandle,
  /** The CertificateKey to use for result signing. */
  val certificateKey: DataProviderCertificateKey,
)

abstract class RequisitionFulfiller(
  protected val dataProviderData: DataProviderData,
  private val certificatesStub: CertificatesCoroutineStub,
  private val requisitionsStub: RequisitionsCoroutineStub,
  protected val throttler: Throttler,
  protected val trustedCertificates: Map<ByteString, X509Certificate>,
) {
  protected data class Specifications(
    val measurementSpec: MeasurementSpec,
    val requisitionSpec: RequisitionSpec,
  )

  protected class InvalidConsentSignalException(message: String? = null, cause: Throwable? = null) :
    GeneralSecurityException(message, cause)

  /** A sequence of operations done in the simulator. */
  abstract suspend fun run()

  /** Executes the requisition fulfillment workflow. */
  abstract suspend fun executeRequisitionFulfillingWorkflow()

  protected fun verifySpecifications(
    requisition: Requisition,
    measurementConsumerCertificate: Certificate,
  ): Specifications {
    val x509Certificate = readCertificate(measurementConsumerCertificate.x509Der)
    // Look up the trusted issuer certificate for this MC certificate. Note that this doesn't
    // confirm that this is the trusted issuer for the right MC. In a production environment,
    // consider having a mapping of MC to root/CA cert.
    val trustedIssuer =
      trustedCertificates[checkNotNull(x509Certificate.authorityKeyIdentifier)]
        ?: throw InvalidConsentSignalException(
          "Issuer of ${measurementConsumerCertificate.name} is not trusted"
        )

    try {
      verifyMeasurementSpec(requisition.measurementSpec, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e,
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException("MeasurementSpec signature is invalid", e)
    }

    val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()

    val publicKey = requisition.dataProviderPublicKey.unpack(EncryptionPublicKey::class.java)!!
    check(publicKey == dataProviderData.privateEncryptionKey.publicKey.toEncryptionPublicKey()) {
      "Unable to decrypt for this public key"
    }
    val signedRequisitionSpec: SignedMessage =
      try {
        decryptRequisitionSpec(
          requisition.encryptedRequisitionSpec,
          dataProviderData.privateEncryptionKey,
        )
      } catch (e: GeneralSecurityException) {
        throw InvalidConsentSignalException("RequisitionSpec decryption failed", e)
      }
    val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()

    try {
      verifyRequisitionSpec(
        signedRequisitionSpec,
        requisitionSpec,
        measurementSpec,
        x509Certificate,
        trustedIssuer,
      )
    } catch (e: CertPathValidatorException) {
      throw InvalidConsentSignalException(
        "Certificate path for ${measurementConsumerCertificate.name} is invalid",
        e,
      )
    } catch (e: SignatureException) {
      throw InvalidConsentSignalException("RequisitionSpec signature is invalid", e)
    } catch (e: NonceMismatchException) {
      throw InvalidConsentSignalException(e.message, e)
    } catch (e: PublicKeyMismatchException) {
      throw InvalidConsentSignalException(e.message, e)
    }

    // TODO(@uakyol): Validate that collection interval is not outside of privacy landscape.

    return Specifications(measurementSpec, requisitionSpec)
  }

  protected suspend fun getCertificate(resourceName: String): Certificate {
    return try {
      certificatesStub.getCertificate(getCertificateRequest { name = resourceName })
    } catch (e: StatusException) {
      throw Exception("Error fetching certificate $resourceName", e)
    }
  }

  protected suspend fun refuseRequisition(
    requisitionName: String,
    justification: Requisition.Refusal.Justification,
    message: String,
    etag: String,
  ): Requisition {
    try {
      return requisitionsStub.refuseRequisition(
        refuseRequisitionRequest {
          name = requisitionName
          refusal = refusal {
            this.justification = justification
            this.message = message
          }
          this.etag = etag
        }
      )
    } catch (e: StatusException) {
      // TODO(world-federation-of-advertisers/cross-media-measurement#2374): Handle ABORT exception
      // by calling GetRequisition.
      throw Exception("Error refusing requisition $requisitionName", e)
    }
  }

  protected suspend fun getRequisitions(): List<Requisition> {
    val request = listRequisitionsRequest {
      parent = dataProviderData.name
      filter = filter { states += Requisition.State.UNFULFILLED }
    }

    try {
      return requisitionsStub.listRequisitions(request).requisitionsList
    } catch (e: StatusException) {
      throw Exception("Error listing requisitions", e)
    }
  }

  protected suspend fun fulfillDirectMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    nonce: Long,
    measurementResult: Measurement.Result,
  ) {
    logger.log(Level.INFO, "Direct MeasurementSpec:\n$measurementSpec")
    logger.log(Level.INFO, "Direct MeasurementResult:\n$measurementResult")

    DataProviderCertificateKey.fromName(requisition.dataProviderCertificate)
      ?: throw RequisitionRefusalException.Default(
        Requisition.Refusal.Justification.UNFULFILLABLE,
        "Invalid data provider certificate",
      )
    val measurementEncryptionPublicKey: EncryptionPublicKey =
      if (measurementSpec.hasMeasurementPublicKey()) {
        measurementSpec.measurementPublicKey.unpack()
      } else {
        @Suppress("DEPRECATION") // Handle legacy resources.
        EncryptionPublicKey.parseFrom(measurementSpec.serializedMeasurementPublicKey)
      }
    val signedResult: SignedMessage =
      signResult(measurementResult, dataProviderData.signingKeyHandle)
    val encryptedResult: EncryptedMessage =
      encryptResult(signedResult, measurementEncryptionPublicKey)

    try {
      requisitionsStub.fulfillDirectRequisition(
        fulfillDirectRequisitionRequest {
          name = requisition.name
          this.encryptedResult = encryptedResult
          this.nonce = nonce
          this.certificate = dataProviderData.certificateKey.toName()
        }
      )
    } catch (e: StatusException) {
      // TODO(world-federation-of-advertisers/cross-media-measurement#2374): Handle ABORT exception
      // by calling GetRequisition.
      throw Exception("Error fulfilling direct requisition ${requisition.name}", e)
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
