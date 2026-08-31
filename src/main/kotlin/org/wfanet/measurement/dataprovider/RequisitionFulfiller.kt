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
import io.grpc.Status
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.delay
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionResponse
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
import org.wfanet.measurement.api.v2alpha.getRequisitionRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.common.NonceMismatchException
import org.wfanet.measurement.consent.client.common.PublicKeyMismatchException
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
  /** Paces outbound calls to the Kingdom's Certificates and Requisitions services. */
  private val kingdomRpcThrottler: Throttler,
  protected val trustedCertificates: Map<ByteString, X509Certificate>,
  private val retryMaxAttempts: Int = DEFAULT_RETRY_MAX_ATTEMPTS,
  private val retryBackoff: ExponentialBackoff = ExponentialBackoff(),
) {
  init {
    require(retryMaxAttempts >= 1) { "retryMaxAttempts must be at least 1" }
  }

  /** Result of checking whether a mutation already took effect on the server. */
  private sealed interface MutationReconciliation<out T> {
    /** The mutation already took effect; [result] is what [callKingdom] should return. */
    data class Applied<T>(val result: T) : MutationReconciliation<T>

    /** The mutation has definitely not taken effect yet and is safe to retry. */
    object NotApplied : MutationReconciliation<Nothing>
  }

  /**
   * Paces [block] via [kingdomRpcThrottler] and retries it with backoff if it throws a
   * [StatusException] with code [Status.Code.UNAVAILABLE] or [Status.Code.ABORTED], up to
   * [retryMaxAttempts] attempts. [errorMessage] is used to wrap the exception from the final
   * attempt if all attempts fail.
   *
   * UNAVAILABLE does not guarantee that [block] never reached or executed on the server -- only
   * that the response was lost. ABORTED means [block] is a mutation whose etag precondition didn't
   * match the current one, which happens both when something else genuinely changed the requisition
   * and when [block]'s own prior attempt already succeeded but its response was lost (leaving the
   * etag advanced from underneath a caller who never saw success). For a non-idempotent mutation,
   * blindly retrying either case risks either a duplicate state transition or a spurious failure on
   * retry. If [block] is such a mutation, pass [reconcileMutation] to check, via
   * [reconcileWithRetry], whether it already took effect before retrying it: if so, its result is
   * returned instead of replaying [block]. [reconcileMutation] is itself retried on UNAVAILABLE,
   * since replaying [block] while unable to confirm whether it already took effect would risk
   * exactly the duplicate-mutation/spurious-failure problem this exists to prevent; any other
   * exception from [reconcileMutation] -- including one it throws to signal that the requisition is
   * in a state that isn't explained by [block] having or not having taken effect -- propagates
   * immediately.
   */
  private suspend fun <T> callKingdom(
    errorMessage: String,
    reconcileMutation: (suspend () -> MutationReconciliation<T>)? = null,
    block: suspend () -> T,
  ): T {
    var attempt = 0
    while (true) {
      attempt++
      try {
        return kingdomRpcThrottler.onReady(block)
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.UNAVAILABLE && e.status.code != Status.Code.ABORTED) {
          throw Exception(errorMessage, e)
        }
        if (reconcileMutation != null) {
          val reconciliation: MutationReconciliation<T> =
            reconcileWithRetry(errorMessage, reconcileMutation)
          if (reconciliation is MutationReconciliation.Applied) {
            return reconciliation.result
          }
        }
        if (attempt >= retryMaxAttempts) {
          throw Exception(errorMessage, e)
        }
        logger.warning {
          "$errorMessage on attempt $attempt of $retryMaxAttempts (${e.message}); retrying"
        }
        delay(retryBackoff.durationForAttempt(attempt).toMillis())
      }
    }
  }

  /**
   * Calls [reconcileMutation], retrying with backoff if it throws a [StatusException] with code
   * [Status.Code.UNAVAILABLE], up to [retryMaxAttempts] attempts. Any other exception propagates
   * immediately: there is no safe default action to take on the original mutation without knowing
   * whether it already took effect.
   */
  private suspend fun <T> reconcileWithRetry(
    errorMessage: String,
    reconcileMutation: suspend () -> MutationReconciliation<T>,
  ): MutationReconciliation<T> {
    var attempt = 0
    while (true) {
      attempt++
      try {
        return kingdomRpcThrottler.onReady(reconcileMutation)
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.UNAVAILABLE || attempt >= retryMaxAttempts) {
          throw Exception("$errorMessage (while confirming whether it already took effect)", e)
        }
        logger.warning {
          "$errorMessage (while confirming whether it already took effect) on attempt $attempt " +
            "of $retryMaxAttempts (${e.message}); retrying"
        }
        delay(retryBackoff.durationForAttempt(attempt).toMillis())
      }
    }
  }

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
    return callKingdom("Error fetching certificate $resourceName") {
      certificatesStub.getCertificate(getCertificateRequest { name = resourceName })
    }
  }

  protected suspend fun refuseRequisition(
    requisitionName: String,
    justification: Requisition.Refusal.Justification,
    message: String,
    etag: String,
  ): Requisition {
    return callKingdom(
      "Error refusing requisition $requisitionName",
      reconcileMutation = {
        val requisition: Requisition =
          requisitionsStub.getRequisition(getRequisitionRequest { name = requisitionName })
        when {
          requisition.state == Requisition.State.REFUSED ->
            MutationReconciliation.Applied(requisition)
          requisition.state == Requisition.State.UNFULFILLED && requisition.etag == etag ->
            MutationReconciliation.NotApplied
          else ->
            error(
              "Unexpected state for requisition $requisitionName while confirming whether " +
                "RefuseRequisition already took effect: state=${requisition.state}, " +
                "etag=${requisition.etag}"
            )
        }
      },
    ) {
      requisitionsStub.refuseRequisition(
        refuseRequisitionRequest {
          name = requisitionName
          refusal = refusal {
            this.justification = justification
            this.message = message
          }
          this.etag = etag
        }
      )
    }
  }

  protected suspend fun getRequisitions(): List<Requisition> {
    val request = listRequisitionsRequest {
      parent = dataProviderData.name
      filter = filter { states += Requisition.State.UNFULFILLED }
    }

    return callKingdom("Error listing requisitions") {
      requisitionsStub.listRequisitions(request).requisitionsList
    }
  }

  /** Fetches the current state of the Requisition with resource name [requisitionName]. */
  protected suspend fun getRequisition(requisitionName: String): Requisition {
    return callKingdom("Error fetching requisition $requisitionName") {
      requisitionsStub.getRequisition(getRequisitionRequest { name = requisitionName })
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

    callKingdom(
      "Error fulfilling direct requisition ${requisition.name}",
      reconcileMutation = {
        val fulfilledRequisition: Requisition =
          requisitionsStub.getRequisition(getRequisitionRequest { name = requisition.name })
        when {
          fulfilledRequisition.state == Requisition.State.FULFILLED ->
            MutationReconciliation.Applied(FulfillDirectRequisitionResponse.getDefaultInstance())
          fulfilledRequisition.state == Requisition.State.UNFULFILLED &&
            fulfilledRequisition.etag == requisition.etag -> MutationReconciliation.NotApplied
          else ->
            error(
              "Unexpected state for requisition ${requisition.name} while confirming whether " +
                "FulfillDirectRequisition already took effect: " +
                "state=${fulfilledRequisition.state}, etag=${fulfilledRequisition.etag}"
            )
        }
      },
    ) {
      requisitionsStub.fulfillDirectRequisition(
        fulfillDirectRequisitionRequest {
          name = requisition.name
          this.encryptedResult = encryptedResult
          this.nonce = nonce
          this.certificate = dataProviderData.certificateKey.toName()
          this.etag = requisition.etag
        }
      )
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val DEFAULT_RETRY_MAX_ATTEMPTS = 4
  }
}
