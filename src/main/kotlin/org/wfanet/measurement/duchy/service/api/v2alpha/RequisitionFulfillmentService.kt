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

package org.wfanet.measurement.duchy.service.api.v2alpha

import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.EncryptionKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest.Header
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest.Header.TrusTee.EnvelopeEncryption.AwsKmsParams.TokenAudienceCase
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.consumeFirst
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.common.telemetry.ReportTracing
import org.wfanet.measurement.consent.client.duchy.Requisition as ConsentSignalingRequisition
import org.wfanet.measurement.consent.client.duchy.verifyRequisitionFulfillment
import org.wfanet.measurement.duchy.storage.RequisitionBlobContext
import org.wfanet.measurement.duchy.storage.RequisitionStore
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.ComputationToken
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey
import org.wfanet.measurement.internal.duchy.GetComputationTokenRequest
import org.wfanet.measurement.internal.duchy.GetComputationTokenResponse
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.RequisitionDetails.RequisitionProtocol.TrusTee.AwsKmsParams.CredentialSource as InternalCredentialSource
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.honestMajorityShareShuffle
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.trusTee
import org.wfanet.measurement.internal.duchy.RequisitionDetailsKt.RequisitionProtocolKt.trusTeeV2
import org.wfanet.measurement.internal.duchy.RequisitionMetadata
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.getComputationTokenRequest
import org.wfanet.measurement.internal.duchy.recordRequisitionFulfillmentRequest
import org.wfanet.measurement.system.v1alpha.RequisitionKey as SystemRequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.fulfillRequisitionRequest as systemFulfillRequisitionRequest

private val FULFILLED_RESPONSE =
  FulfillRequisitionResponse.newBuilder().apply { state = Requisition.State.FULFILLED }.build()

/** Implementation of `wfa.measurement.api.v2alpha.RequisitionFulfillment` gRPC service. */
class RequisitionFulfillmentService(
  private val duchyId: String,
  private val systemRequisitionsClient: RequisitionsCoroutineStub,
  private val computationsClient: ComputationsCoroutineStub,
  private val requisitionStore: RequisitionStore,
  coroutineContext: CoroutineContext,
) : RequisitionFulfillmentCoroutineImplBase(coroutineContext) {

  private enum class Permission {
    FULFILL;

    fun deniedStatus(name: String): Status {
      return Status.PERMISSION_DENIED.withDescription(
        "Permission $this denied on resource $name (or it might not exist)."
      )
    }
  }

  override suspend fun fulfillRequisition(
    requests: Flow<FulfillRequisitionRequest>
  ): FulfillRequisitionResponse {
    grpcRequireNotNull(requests.consumeFirst()) { "Empty request stream" }
      .use { consumed ->
        val header = consumed.item.header
        val key =
          grpcRequireNotNull(CanonicalRequisitionKey.fromName(header.name)) {
            "Resource name unspecified or invalid."
          }
        val externalRequisitionKey = externalRequisitionKey {
          externalRequisitionId = key.requisitionId
          requisitionFingerprint = header.requisitionFingerprint
        }
        val traceAttributes =
          Attributes.builder()
            .put(ReportTraceAttributes.REQUISITION_NAME, header.name)
            .put(ReportTraceAttributes.DUCHY_ID, duchyId)
            .build()
        val computationToken =
          ReportTracing.traceSuspending(
            spanName = "duchy.requisition.acceptance",
            attributes =
              traceAttributes
                .toBuilder()
                .put(ReportTraceAttributes.LIFECYCLE_STAGE, "duchy_requisition_acceptance")
                .put(ReportTraceAttributes.OUTCOME, "started")
                .build(),
          ) {
            // A multimeasurement fulfillment carries no nonce.
            if (header.hasTrusTeeV2()) {
              grpcRequire(header.nonce == 0L) { "nonce specified for a TrusTEE v2 fulfillment" }
            } else {
              grpcRequire(header.nonce != 0L) { "nonce unspecified" }
            }

            val authenticatedPrincipal = principalFromCurrentContext
            if (key.parentKey != authenticatedPrincipal.resourceKey) {
              throw Permission.FULFILL.deniedStatus(header.name).asRuntimeException()
            }

            val token = getComputationToken(externalRequisitionKey)
            val requisitionMetadata =
              if (token.computationStage.stageCase == ComputationStage.StageCase.TRUS_TEE_V2) {
                // The plaintext RequisitionSpec of a multimeasurement has no nonce to verify. Its
                // fingerprint already matched in the computation lookup.
                checkNotNull(
                  token.requisitionsList.find { it.externalKey == externalRequisitionKey }
                )
              } else {
                verifyRequisitionFulfillment(token, externalRequisitionKey, header.nonce)
              }

            // Only try writing to the blob store if it is not already marked fulfilled.
            // TODO(world-federation-of-advertisers/cross-media-measurement#85): Handle
            // the case that
            //  it is already marked fulfilled locally.
            if (requisitionMetadata.path.isBlank()) {
              val blob =
                requisitionStore.write(
                  RequisitionBlobContext(token.globalComputationId, key.requisitionId),
                  consumed.remaining.map { it.bodyChunk.data },
                )
              when (token.computationStage.stageCase) {
                ComputationStage.StageCase.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2,
                ComputationStage.StageCase.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 -> {
                  recordLlv2RequisitionLocally(token, externalRequisitionKey, blob.blobKey)
                }
                ComputationStage.StageCase.HONEST_MAJORITY_SHARE_SHUFFLE -> {
                  val hmss = header.honestMajorityShareShuffle
                  grpcRequire(hmss.hasSecretSeed()) {
                    "Secret seed not specified for HMSS protocol."
                  }
                  grpcRequire(hmss.dataProviderCertificate.isNotBlank()) {
                    "DataProviderCertificate not specified for HMSS protocol."
                  }
                  val fulfillingDuchyId = requisitionMetadata.details.externalFulfillingDuchyId
                  grpcRequire(fulfillingDuchyId == duchyId) {
                    "FulfillingDuchyId mismatch. fulfillingDuchyId=$fulfillingDuchyId, " +
                      "currentDuchy=$duchyId"
                  }

                  recordHmssRequisitionLocally(
                    token = token,
                    key = externalRequisitionKey,
                    blobPath = blob.blobKey,
                    secretSeedCiphertext = hmss.secretSeed.ciphertext,
                    registerCount = hmss.registerCount,
                    dataProviderCertificate = hmss.dataProviderCertificate,
                  )
                }
                ComputationStage.StageCase.TRUS_TEE -> {
                  recordTrusTeeRequisitionLocally(
                    token = token,
                    key = externalRequisitionKey,
                    blobPath = blob.blobKey,
                    protocolDetails =
                      RequisitionDetailsKt.requisitionProtocol {
                        trusTee = header.trusTee.toInternal()
                      },
                  )
                }
                ComputationStage.StageCase.TRUS_TEE_V2 -> {
                  grpcRequire(header.hasTrusTeeV2()) {
                    "trus_tee_v2 not specified for a TrusTEE v2 computation."
                  }
                  recordTrusTeeRequisitionLocally(
                    token = token,
                    key = externalRequisitionKey,
                    blobPath = blob.blobKey,
                    protocolDetails =
                      RequisitionDetailsKt.requisitionProtocol {
                        trusTeeV2 = header.trusTeeV2.toInternal(key.dataProviderId)
                      },
                  )
                }
                ComputationStage.StageCase.STAGE_NOT_SET -> failGrpc { "ComputationStage not set" }
              }
            }

            Span.current().setAttribute(ReportTraceAttributes.OUTCOME, "succeeded")
            token
          }

        ReportTracing.traceSuspending(
          spanName = "duchy.requisition.kingdom_fulfillment",
          attributes =
            traceAttributes
              .toBuilder()
              .put(ReportTraceAttributes.LIFECYCLE_STAGE, "duchy_requisition_kingdom_fulfillment")
              .put(ReportTraceAttributes.OUTCOME, "started")
              .build(),
        ) {
          fulfillRequisitionAtKingdom(
            computationToken.globalComputationId,
            externalRequisitionKey.externalRequisitionId,
            header.nonce,
            header.etag,
          )
          Span.current().setAttribute(ReportTraceAttributes.OUTCOME, "succeeded")
        }

        return FULFILLED_RESPONSE
      }
  }

  private suspend fun getComputationToken(
    requisitionKey: ExternalRequisitionKey
  ): ComputationToken {
    return getComputationToken(getComputationTokenRequest { this.requisitionKey = requisitionKey })
      .token
  }

  /** Sends a request to get computation token. */
  private suspend fun getComputationToken(
    request: GetComputationTokenRequest
  ): GetComputationTokenResponse {
    return try {
      computationsClient.getComputationToken(request)
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.NOT_FOUND) {
        throw Status.NOT_FOUND.withDescription(
            "No computation is expecting this requisition $this."
          )
          .asRuntimeException()
      } else {
        throw Status.INTERNAL.withCause(e).asRuntimeException()
      }
    }
  }

  private fun verifyRequisitionFulfillment(
    computationToken: ComputationToken,
    requisitionKey: ExternalRequisitionKey,
    nonce: Long,
  ): RequisitionMetadata {
    val kingdomComputation = computationToken.computationDetails.kingdomComputation
    val requisitionMetadata =
      checkNotNull(computationToken.requisitionsList.find { it.externalKey == requisitionKey })

    val publicApiVersion =
      Version.fromStringOrNull(kingdomComputation.publicApiVersion)
        ?: throw Status.FAILED_PRECONDITION.withDescription(
            "Public API version invalid or unspecified"
          )
          .asRuntimeException()
    when (publicApiVersion) {
      Version.V2_ALPHA -> {
        val measurementSpec = MeasurementSpec.parseFrom(kingdomComputation.measurementSpec)
        if (
          !verifyRequisitionFulfillment(
            measurementSpec,
            requisitionMetadata.toConsentSignalingRequisition(),
            requisitionKey.requisitionFingerprint,
            nonce,
          )
        ) {
          throw Status.FAILED_PRECONDITION.withDescription(
              "Requisition fulfillment could not be verified"
            )
            .asRuntimeException()
        }
      }
    }

    return requisitionMetadata
  }

  /** Sends rpc to the duchy's internal ComputationsService to record Llv2 requisition */
  private suspend fun recordLlv2RequisitionLocally(
    token: ComputationToken,
    key: ExternalRequisitionKey,
    blobPath: String,
  ) {
    computationsClient.recordRequisitionFulfillment(
      recordRequisitionFulfillmentRequest {
        this.token = token
        this.key = key
        this.blobPath = blobPath
        publicApiVersion = Version.V2_ALPHA.string
      }
    )
  }

  /** Sends rpc to the duchy's internal ComputationsService to record Hmss requisition */
  private suspend fun recordHmssRequisitionLocally(
    token: ComputationToken,
    key: ExternalRequisitionKey,
    blobPath: String,
    secretSeedCiphertext: ByteString,
    registerCount: Long,
    dataProviderCertificate: String,
  ) {
    computationsClient.recordRequisitionFulfillment(
      recordRequisitionFulfillmentRequest {
        this.token = token
        this.key = key
        this.blobPath = blobPath
        publicApiVersion = Version.V2_ALPHA.string
        protocolDetails =
          RequisitionDetailsKt.requisitionProtocol {
            honestMajorityShareShuffle = honestMajorityShareShuffle {
              this.secretSeedCiphertext = secretSeedCiphertext
              this.registerCount = registerCount
              this.dataProviderCertificate = dataProviderCertificate
            }
          }
      }
    )
  }

  /** Sends rpc to the duchy's internal ComputationsService to record a TrusTee requisition. */
  private suspend fun recordTrusTeeRequisitionLocally(
    token: ComputationToken,
    key: ExternalRequisitionKey,
    blobPath: String,
    protocolDetails: RequisitionDetails.RequisitionProtocol,
  ) {
    computationsClient.recordRequisitionFulfillment(
      recordRequisitionFulfillmentRequest {
        this.token = token
        this.key = key
        this.blobPath = blobPath
        publicApiVersion = Version.V2_ALPHA.string
        this.protocolDetails = protocolDetails
      }
    )
  }

  private fun Header.TrusTee.toInternal(): RequisitionDetails.RequisitionProtocol.TrusTee {
    val source = this
    return trusTee {
      populationSpecFingerprint = source.populationSpecFingerprint
      when (source.dataFormat) {
        Header.TrusTee.DataFormat.FREQUENCY_VECTOR -> {
          dataFormat = RequisitionDetails.RequisitionProtocol.TrusTee.DataFormat.FREQUENCY_VECTOR
        }
        Header.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR -> {
          val envelopeEncryption = source.envelopeEncryption
          when (envelopeEncryption.encryptedDek.format) {
            EncryptionKey.Format.TINK_ENCRYPTED_KEYSET -> {}
            EncryptionKey.Format.FORMAT_UNSPECIFIED,
            EncryptionKey.Format.UNRECOGNIZED -> failGrpc { "Invalid EncryptedDek format" }
          }
          dataFormat =
            RequisitionDetails.RequisitionProtocol.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR
          encryptedDekCiphertext = envelopeEncryption.encryptedDek.data
          kmsKekUri = envelopeEncryption.kmsKekUri
          workloadIdentityProvider = envelopeEncryption.workloadIdentityProvider
          impersonatedServiceAccount = envelopeEncryption.impersonatedServiceAccount
          if (envelopeEncryption.hasAwsKmsParams()) {
            val apiAwsKmsParams = envelopeEncryption.awsKmsParams
            // Which audience is set determines how the mill authenticates to
            // AWS STS.
            val (awsAudience, awsCredentialSource) =
              when (apiAwsKmsParams.tokenAudienceCase) {
                TokenAudienceCase.WORKLOAD_IDENTITY_ID_TOKEN_AUDIENCE ->
                  apiAwsKmsParams.workloadIdentityIdTokenAudience to
                    InternalCredentialSource.GCP_WORKLOAD_IDENTITY
                TokenAudienceCase.CONFIDENTIAL_SPACE_ATTESTATION_TOKEN_AUDIENCE ->
                  apiAwsKmsParams.confidentialSpaceAttestationTokenAudience to
                    InternalCredentialSource.CONFIDENTIAL_SPACE
                TokenAudienceCase.TOKENAUDIENCE_NOT_SET ->
                  failGrpc { "trus_tee.envelope_encryption.aws_kms_params.token_audience not set" }
              }
            awsKmsParams =
              RequisitionDetailsKt.RequisitionProtocolKt.TrusTeeKt.awsKmsParams {
                roleArn = apiAwsKmsParams.roleArn
                roleSession = apiAwsKmsParams.roleSession
                region = apiAwsKmsParams.region
                audience = awsAudience
                credentialSource = awsCredentialSource
              }
          }
        }
        Header.TrusTee.DataFormat.DATA_FORMAT_UNSPECIFIED,
        Header.TrusTee.DataFormat.UNRECOGNIZED -> failGrpc { "Unsupported data format." }
      }
    }
  }

  private fun Header.TrusTeeV2.toInternal(
    dataProviderResourceId: String
  ): RequisitionDetails.RequisitionProtocol.TrusTeeV2 {
    val source = this
    return trusTeeV2 {
      trusTee = source.trusTee.toInternal()
      this.dataProviderResourceId = dataProviderResourceId
      when (source.detailsCase) {
        Header.TrusTeeV2.DetailsCase.FULFILLMENT_DETAILS -> {
          grpcRequire(source.trusTee.dataFormat == Header.TrusTee.DataFormat.FREQUENCY_VECTOR) {
            "fulfillment_details requires the FREQUENCY_VECTOR data format."
          }
          fulfillmentDetails = source.fulfillmentDetails.toByteString()
        }
        Header.TrusTeeV2.DetailsCase.ENCRYPTED_FULFILLMENT_DETAILS -> {
          grpcRequire(
            source.trusTee.dataFormat == Header.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR
          ) {
            "encrypted_fulfillment_details requires the ENCRYPTED_FREQUENCY_VECTOR data format."
          }
          encryptedFulfillmentDetails = source.encryptedFulfillmentDetails.toByteString()
        }
        Header.TrusTeeV2.DetailsCase.DETAILS_NOT_SET -> {}
      }
    }
  }

  /** send rpc to the kingdom's system RequisitionsService to fulfill a requisition. */
  private suspend fun fulfillRequisitionAtKingdom(
    computationId: String,
    requisitionId: String,
    nonce: Long,
    etag: String,
  ) {
    systemRequisitionsClient.fulfillRequisition(
      systemFulfillRequisitionRequest {
        name = SystemRequisitionKey(computationId, requisitionId).toName()
        this.nonce = nonce
        this.etag = etag
      }
    )
  }
}

private fun RequisitionMetadata.toConsentSignalingRequisition() =
  ConsentSignalingRequisition(externalKey.requisitionFingerprint, details.nonceHash)
