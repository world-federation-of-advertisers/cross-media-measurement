package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.unpack
import com.google.type.Interval
import io.grpc.StatusException
import java.io.File
import java.security.GeneralSecurityException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.map
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
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
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.sampling.VidSampler
import org.wfanet.measurement.storage.BlobUri

/**
 * Helper functions for measurement operations.
 */
class MeasurementHelper(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val dataProviderSigningKeyHandle: SigningKeyHandle,
  private val random: Random = Random,
  private val dataProviderCertificateKey: DataProviderCertificateKey
) {
  private val logger: Logger = Logger.getLogger(this::class.java.name)

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
} 
