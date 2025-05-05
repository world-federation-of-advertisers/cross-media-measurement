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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.storage.SelectedStorageClient

class ResultsFulfiller(
  private val privateEncryptionKey: PrivateKeyHandle,
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val dataProviderCertificateKey: DataProviderCertificateKey,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val typeRegistry: TypeRegistry,
  private val requisitionsBlobUri: String,
  private val labeledImpressionMetadataPrefix: String,
  private val kmsClient: KmsClient,
  private val impressionsStorageConfig: StorageConfig,
  private val impressionMetadataStorageConfig: StorageConfig,
  private val requisitionsStorageConfig: StorageConfig,
  private val random: Random = Random,
) {
  suspend fun fulfillRequisitions() {
    val requisitions = getRequisitions()
    requisitions.collect { requisition ->
      val signedRequisitionSpec: SignedMessage =
        try {
          decryptRequisitionSpec(
            requisition.encryptedRequisitionSpec,
            privateEncryptionKey,
          )
        } catch (e: GeneralSecurityException) {
          throw Exception("RequisitionSpec decryption failed", e)
        }
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()

      val sampledVids = VidUtils.getSampledVids(
        requisitionSpec,
        measurementSpec.vidSamplingInterval,
        typeRegistry,
        kmsClient,
        impressionsStorageConfig,
        impressionMetadataStorageConfig,
        labeledImpressionMetadataPrefix,
      )
      val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList

      if (protocols.any { it.hasDirect() }) {
        val directProtocolConfig =
          requisition.protocolConfig.protocolsList.first { it.hasDirect() }.direct
        val directNoiseMechanismOptions =
          directProtocolConfig.noiseMechanismsList
            .mapNotNull { protocolConfigNoiseMechanism ->
              protocolConfigNoiseMechanism.toDirectNoiseMechanism()
            }
            .toSet()
        if (measurementSpec.hasReach() || measurementSpec.hasReachAndFrequency()) {
          fulfillDirectReachAndFrequencyMeasurement(
            requisition,
            measurementSpec,
            sampledVids,
            directProtocolConfig,
            selectReachAndFrequencyNoiseMechanism(directNoiseMechanismOptions),
            requisitionSpec.nonce,
          )
        } else if (measurementSpec.hasDuration()) {
          TODO("Not yet implemented")
        } else if (measurementSpec.hasImpression()) {
          TODO("Not yet implemented")
        } else {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Measurement type not supported for direct fulfillment.",
          )
        }
      } else if (protocols.any { it.hasLiquidLegionsV2() }) {
        TODO("Not yet implemented")
      } else if (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
        TODO("Not yet implemented")
      } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
        TODO("Not yet implemented")
      } else {
        throw Exception("Protocol not supported")
      }
    }
  }

  /**
   * Selects the most preferred [DirectNoiseMechanism] for reach and frequency measurements from the
   * overlap of a list of preferred [DirectNoiseMechanism] and a set of [DirectNoiseMechanism]
   * [options].
   */
  private fun selectReachAndFrequencyNoiseMechanism(
    options: Set<DirectNoiseMechanism>
  ): DirectNoiseMechanism {
    val preferences = listOf(DirectNoiseMechanism.CONTINUOUS_GAUSSIAN)
    return preferences.firstOrNull { preference -> options.contains(preference) }
      ?: throw Exception(
        "No valid noise mechanism option for reach or frequency measurements.",
      )
  }

  private suspend fun fulfillDirectReachAndFrequencyMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    sampledVids: Flow<Long>,
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism,
    nonce: Long,
  ) {
    logger.info("Calculating direct reach and frequency...")
    val measurementResult = buildDirectMeasurementResult(
      directProtocolConfig,
      directNoiseMechanism,
      measurementSpec,
      sampledVids,
    )

    fulfillDirectMeasurement(requisition, measurementSpec, nonce, measurementResult)
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
      ?: throw Exception(
        "Invalid data provider certificate"
      )
    val measurementEncryptionPublicKey: EncryptionPublicKey =
      if (measurementSpec.hasMeasurementPublicKey()) {
        measurementSpec.measurementPublicKey.unpack()
      } else {
        @Suppress("DEPRECATION") // Handle legacy resources.
        EncryptionPublicKey.parseFrom(measurementSpec.serializedMeasurementPublicKey)
      }
    val signedResult: SignedMessage =
      signResult(measurementResult, dataProviderSigningKeyHandle)
    val encryptedResult: EncryptedMessage =
      encryptResult(signedResult, measurementEncryptionPublicKey)

    try {
      requisitionsStub.fulfillDirectRequisition(
        fulfillDirectRequisitionRequest {
          name = requisition.name
          this.encryptedResult = encryptedResult
          this.nonce = nonce
          this.certificate = dataProviderCertificateKey.toName()
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error fulfilling direct requisition ${requisition.name}", e)
    }
  }

  /**
   * Build [Measurement.Result] of the measurement type specified in [MeasurementSpec].
   *
   * @param measurementSpec Measurement spec.
   * @param samples sampled events.
   * @return [Measurement.Result].
   */
  private suspend fun buildDirectMeasurementResult(
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism,
    measurementSpec: MeasurementSpec,
    samples: Flow<Long>,
  ): Measurement.Result {
    val protocolConfigNoiseMechanism = when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE -> {
        NoiseMechanism.NONE
      }
      DirectNoiseMechanism.CONTINUOUS_LAPLACE -> {
        NoiseMechanism.CONTINUOUS_LAPLACE
      }
      else -> {
        NoiseMechanism.CONTINUOUS_GAUSSIAN
      }
    }

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum fields cannot be null.
    return when (measurementSpec.measurementTypeCase) {
      MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY -> {
        if (!directProtocolConfig.hasDeterministicCountDistinct()) {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for direct reach computation.",
          )
        }
        if (!directProtocolConfig.hasDeterministicDistribution()) {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for direct frequency distribution computation.",
          )
        }

        val (sampledReachValue, frequencyMap) =
          MeasurementResults.computeReachAndFrequency(
            samples,
            measurementSpec.reachAndFrequency.maximumFrequency,
          )

        logger.info("Adding $directNoiseMechanism publisher noise to direct reach and frequency...")
        val sampledNoisedReachValue =
          addReachPublisherNoise(
            sampledReachValue,
            measurementSpec.reachAndFrequency.reachPrivacyParams,
            directNoiseMechanism,
          )
        val noisedFrequencyMap =
          addFrequencyPublisherNoise(
            sampledReachValue,
            frequencyMap,
            measurementSpec.reachAndFrequency.frequencyPrivacyParams,
            directNoiseMechanism,
          )

        val scaledNoisedReachValue =
          (sampledNoisedReachValue / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach {
            value = scaledNoisedReachValue
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
          }
          frequency = MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(noisedFrequencyMap.mapKeys { it.key.toLong() })
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicDistribution = DeterministicDistribution.getDefaultInstance()
          }
        }
      }

      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.DURATION -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.REACH -> {
        MeasurementKt.result {
          TODO("Not yet implemented")
        }
      }

      MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET -> {
        error("Measurement type not set.")
      }
    }
  }

  private fun getPublisherNoiser(
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
    random: Random,
  ): AbstractNoiser =
    when (directNoiseMechanism) {
      DirectNoiseMechanism.NONE ->
        object : AbstractNoiser() {
          override val distribution = ConstantRealDistribution(0.0)
          override val variance: Double
            get() = distribution.numericalVariance
        }

      DirectNoiseMechanism.CONTINUOUS_LAPLACE ->
        LaplaceNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())

      DirectNoiseMechanism.CONTINUOUS_GAUSSIAN ->
        GaussianNoiser(DpParams(privacyParams.epsilon, privacyParams.delta), random.asJavaRandom())
    }

  /**
   * Add publisher noise to calculated direct reach.
   *
   * @param reachValue Direct reach value.
   * @param privacyParams Differential privacy params for reach.
   * @param directNoiseMechanism Selected noise mechanism for direct reach.
   * @return Noised non-negative reach value.
   */
  private fun addReachPublisherNoise(
    reachValue: Int,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
  ): Int {
    val reachNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    return max(0, reachValue + reachNoiser.sample().toInt())
  }

  /**
   * Add publisher noise to calculated direct frequency.
   *
   * @param reachValue Direct reach value.
   * @param frequencyMap Direct frequency.
   * @param privacyParams Differential privacy params for frequency map.
   * @param directNoiseMechanism Selected noise mechanism for direct frequency.
   * @return Noised non-negative frequency map.
   */
  private fun addFrequencyPublisherNoise(
    reachValue: Int,
    frequencyMap: Map<Int, Double>,
    privacyParams: DifferentialPrivacyParams,
    directNoiseMechanism: DirectNoiseMechanism,
  ): Map<Int, Double> {
    val frequencyNoiser: AbstractNoiser =
      getPublisherNoiser(privacyParams, directNoiseMechanism, random)

    // Add noise to the histogram and cap negative values to zeros.
    val frequencyHistogram: Map<Int, Int> =
      frequencyMap.mapValues { (_, percentage) ->
        // Round the noise for privacy.
        val noisedCount: Int =
          (percentage * reachValue).roundToInt() + (frequencyNoiser.sample()).roundToInt()
        max(0, noisedCount)
      }
    val normalizationTerm: Double = frequencyHistogram.values.sum().toDouble()
    // Normalize to get the distribution
    return if (normalizationTerm != 0.0) {
      frequencyHistogram.mapValues { (_, count) -> count / normalizationTerm }
    } else {
      frequencyHistogram.mapValues { 0.0 }
    }
  }

  /**
   * Converts a [NoiseMechanism] to a nullable [DirectNoiseMechanism].
   *
   * @return [DirectNoiseMechanism] when there is a matched, otherwise null.
   */
  private fun NoiseMechanism.toDirectNoiseMechanism(): DirectNoiseMechanism? {
    return when (this) {
      NoiseMechanism.NONE -> DirectNoiseMechanism.NONE
      NoiseMechanism.CONTINUOUS_LAPLACE -> DirectNoiseMechanism.CONTINUOUS_LAPLACE
      NoiseMechanism.CONTINUOUS_GAUSSIAN -> DirectNoiseMechanism.CONTINUOUS_GAUSSIAN
      NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
      NoiseMechanism.GEOMETRIC,
      NoiseMechanism.DISCRETE_GAUSSIAN,
      NoiseMechanism.UNRECOGNIZED -> {
        null
      }
    }
  }

  /**
   * Retrieves a list of requisitions from the configured blob storage.
   *
   * This method performs the following operations:
   * 1. Parses the requisitions blob URI to create a storage client
   * 2. Fetches the requisition blob from storage
   * 3. Reads and concatenates all data from the blob
   * 4. Parses the UTF-8 encoded string data into a Requisition object using TextFormat
   *
   * @return A Flow containing the single requisition retrieved from blob storage
   * @throws NullPointerException If the requisition blob cannot be found at the specified URI
   */
  private suspend fun getRequisitions(): Flow<Requisition> {
    // Create storage client based on blob URI
    val storageClientUri = SelectedStorageClient.parseBlobUri(requisitionsBlobUri)
    val requisitionsStorageClient = VidUtils.createStorageClient(storageClientUri, requisitionsStorageConfig)

    // TODO(@jojijac0b): Refactor once grouped requisitions are supported
    val requisition = Requisition.parseFrom(requisitionsStorageClient.getBlob(storageClientUri.key)!!.read().flatten())

    return listOf(requisition).asFlow()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** [RequisitionRefusalException] for EventGroups. */
    protected open class RequisitionRefusalException(
      val justification: Requisition.Refusal.Justification,
      message: String,
      cause: Throwable? = null,
    ) : Exception(message, cause) {
      override val message: String
        get() = super.message!!
    }
  }
}
