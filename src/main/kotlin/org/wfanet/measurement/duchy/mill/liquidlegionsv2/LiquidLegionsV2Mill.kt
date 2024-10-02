// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.liquidlegionsv2

import com.google.protobuf.ByteString
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.consent.client.duchy.verifyDataProviderParticipation
import org.wfanet.measurement.consent.client.duchy.verifyElGamalPublicKey
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.internal.duchy.ComputationDetails.KingdomComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.ComputationDetails.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt
import org.wfanet.measurement.system.v1alpha.confirmComputationParticipantRequest

/**
 * Parent mill of ReachOnlyLiquidLegionsV2 and ReachFrequencyLiquidLegionsV2.
 *
 * @param millId The identifier of this mill, used to claim a work.
 * @param duchyId The identifier of this duchy who owns this mill.
 * @param signingKey handle to a signing private key for consent signaling.
 * @param consentSignalCert The [Certificate] used for consent signaling.
 * @param trustedCertificates [Map] of SKID to trusted certificate
 * @param dataClients clients that have access to local computation storage, i.e., spanner table and
 *   blob store.
 * @param systemComputationParticipantsClient client of the kingdom's system
 *   ComputationParticipantsService.
 * @param systemComputationsClient client of the kingdom's system computationsService.
 * @param systemComputationLogEntriesClient client of the kingdom's system
 *   computationLogEntriesService.
 * @param computationStatsClient client of the duchy's internal ComputationStatsService.
 * @param computationType The [ComputationType] this mill is working on.
 * @param requestChunkSizeBytes The size of data chunk when sending result to other duchies.
 * @param clock A clock
 * @param maximumAttempts The maximum number of attempts on a computation at the same stage.
 * @param workerStubs A map from other duchies' Ids to their corresponding
 *   computationControlClients, used for passing computation to other duchies.
 */
abstract class LiquidLegionsV2Mill(
  millId: String,
  duchyId: String,
  signingKey: SigningKeyHandle,
  consentSignalCert: Certificate,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  dataClients: ComputationDataClients,
  systemComputationParticipantsClient: ComputationParticipantsCoroutineStub,
  systemComputationsClient: ComputationsGrpcKt.ComputationsCoroutineStub,
  systemComputationLogEntriesClient: ComputationLogEntriesCoroutineStub,
  computationStatsClient: ComputationStatsCoroutineStub,
  computationType: ComputationType,
  private val workerStubs: Map<String, ComputationControlCoroutineStub>,
  workLockDuration: Duration,
  requestChunkSizeBytes: Int = 1024 * 32,
  maximumAttempts: Int = 10,
  clock: Clock = Clock.systemUTC(),
) :
  MillBase(
    millId,
    duchyId,
    signingKey,
    consentSignalCert,
    dataClients,
    systemComputationParticipantsClient,
    systemComputationsClient,
    systemComputationLogEntriesClient,
    computationStatsClient,
    computationType,
    workLockDuration,
    requestChunkSizeBytes,
    maximumAttempts,
    clock,
  ) {

  /**
   * Verifies that all EDPs have participated.
   *
   * @return a list of error messages if anything is wrong, otherwise an empty list.
   */
  protected fun verifyEdpParticipation(
    details: KingdomComputationDetails,
    requisitions: Iterable<RequisitionMetadata>,
  ): List<String> {

    val errorList = mutableListOf<String>()
    when (Version.fromString(details.publicApiVersion)) {
      Version.V2_ALPHA -> {
        val measurementSpec = MeasurementSpec.parseFrom(details.measurementSpec)
        if (
          !verifyDataProviderParticipation(measurementSpec, requisitions.map { it.details.nonce })
        ) {
          errorList.add("Cannot verify participation of all DataProviders.")
        }
      }
    }
    for (requisition in requisitions) {
      if (requisition.details.externalFulfillingDuchyId == duchyId && requisition.path.isBlank()) {
        errorList.add(
          "Missing expected data for requisition ${requisition.externalKey.externalRequisitionId}."
        )
      }
    }
    return errorList
  }

  /**
   * Verifies the ElGamal public key of [duchy].
   *
   * @return the error message if verification fails, or else `null`
   */
  protected fun verifyDuchySignature(
    duchy: InternalComputationParticipant,
    publicApiVersion: Version,
  ): String? {
    val duchyInfo: DuchyInfo.Entry =
      requireNotNull(DuchyInfo.getByDuchyId(duchy.duchyId)) {
        "DuchyInfo not found for ${duchy.duchyId}"
      }
    val duchyCertificate: X509Certificate = readCertificate(duchy.duchyCertificateDer)
    val signatureAlgorithmOid =
      duchy.elGamalPublicKeySignatureAlgorithmOid.ifEmpty { duchyCertificate.sigAlgOID }
    val signatureAlgorithm =
      requireNotNull(SignatureAlgorithm.fromOid(signatureAlgorithmOid)) {
        "Unsupported signature algorithm OID $signatureAlgorithmOid"
      }
    when (publicApiVersion) {
      Version.V2_ALPHA -> {
        try {
          verifyElGamalPublicKey(
            duchy.elGamalPublicKey,
            duchy.elGamalPublicKeySignature,
            signatureAlgorithm,
            duchyCertificate,
            trustedCertificates.getValue(duchyInfo.rootCertificateSkid),
          )
        } catch (e: CertPathValidatorException) {
          return "Certificate path invalid for Duchy ${duchy.duchyId}"
        } catch (e: SignatureException) {
          return "Invalid ElGamal public key signature for Duchy ${duchy.duchyId}"
        }
      }
    }
    return null
  }

  protected suspend fun confirmComputationParticipant(token: ComputationToken) {
    updateComputationParticipant(token) { participant: ComputationParticipant ->
      if (participant.state == ComputationParticipant.State.READY) {
        logger.warning {
          val globalComputationId = token.globalComputationId
          "Skipping ConfirmComputationParticipant for $globalComputationId: already ready"
        }
        return@updateComputationParticipant
      }

      systemComputationParticipantsClient.confirmComputationParticipant(
        confirmComputationParticipantRequest {
          name = participant.name
          etag = participant.etag
        }
      )
    }
  }

  /** Fails a computation both locally and at the kingdom when the confirmation fails. */
  protected fun failComputationAtConfirmationPhase(
    token: ComputationToken,
    errorList: List<String>,
  ): ComputationToken {
    val errorMessage =
      "@Mill $millId, Computation ${token.globalComputationId} failed due to:\n" +
        errorList.joinToString(separator = "\n")
    throw ComputationDataClients.PermanentErrorException(errorMessage)
  }

  protected fun nextDuchyId(duchyList: List<InternalComputationParticipant>): String {
    val index = duchyList.indexOfFirst { it.duchyId == duchyId }
    return duchyList[(index + 1) % duchyList.size].duchyId
  }

  protected fun aggregatorDuchyStub(aggregatorId: String): ComputationControlCoroutineStub {
    return workerStubs[aggregatorId]
      ?: throw ComputationDataClients.PermanentErrorException(
        "No ComputationControlService stub for the Aggregator duchy '$aggregatorId'"
      )
  }

  protected val ComputationToken.participantCount: Int
    get() =
      if (computationDetails.kingdomComputation.participantCount != 0) {
        computationDetails.kingdomComputation.participantCount
      } else {
        // For legacy Computations. See world-federation-of-advertisers/cross-media-measurement#1194
        workerStubs.size + 1
      }

  companion object {
    init {
      System.loadLibrary("estimators")
      System.loadLibrary("sketch_encrypter_adapter")
    }

    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
