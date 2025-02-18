package org.wfanet.measurement.securecomputation.teeapps.resultsfulfillment

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import com.google.crypto.tink.tinkkey.KeyHandle
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.duration
import com.google.protobuf.kotlin.unpack
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.log2
import kotlin.math.max
import kotlin.math.roundToInt
import kotlin.random.Random
import kotlin.random.asJavaRandom
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.flow.toList
import org.apache.commons.math3.distribution.ConstantRealDistribution
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CustomDirectMethodologyKt.variance
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DeterministicCount
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.frequency
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.watchDuration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.customDirectMethodology
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.dataprovider.RequisitionFulfiller
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.loadtest.config.VidSampling
import org.wfanet.measurement.loadtest.dataprovider.EdpSimulator
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.SketchGenerator
import org.wfanet.measurement.loadtest.dataprovider.toSketchConfig
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EncryptedDEK
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig.TriggeredApp
import org.wfanet.measurement.securecomputation.resultsfulfillment.MeasurementResults
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.RequisitionsList
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.TeeAppConfig
import org.wfanet.sampling.VidSampler
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.common.MetaLabelerOutput
import org.wfanet.virtualpeople.common.VirtualPersonActivity


data class EventGroupData(
  /** The EventGroups's key. */
  val eventGroupKey: EventGroupKey,
  /** The EventGroups's collection interval. */
  val ds: String,
  /** The prefix of labelled impressions in ShardedStorage. */
  val prefix: String,
)

class ResultFulfiller(
  private val storageClient: StorageClient,
  private val shardedStorageClient: StorageClient,
  val privateEncryptionKey: PrivateKeyHandle,
  val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  val dataProviderCertificateKey: DataProviderCertificateKey?,
  val dataProviderSigningKeyHandle: SigningKeyHandle?,
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<TriggeredApp>,
  private val random: Random = Random,
  ): BaseTeeApplication<TriggeredApp>(subscriptionId,queueSubscriber,parser) {

  override suspend fun runWork(message: TriggeredApp) {
    val teeAppConfig = message.config.unpack(TeeAppConfig::class.java)
    assert(teeAppConfig.workTypeCase == TeeAppConfig.WorkTypeCase.REACH_AND_FREQUENCY_CONFIG)
    val reachAndFrequencyConfig = teeAppConfig.reachAndFrequencyConfig

    // Gets path to list of new stored requisitions in blob storage
    val requisitionsBlob = storageClient.getBlob(message.path)!!
    val requisitionsData =
      requisitionsBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
    val requisitions = RequisitionsList.getDefaultInstance()
      .newBuilderForType()
      .apply { TextFormat.Parser.newBuilder().build().merge(requisitionsData, this) }
      .build() as RequisitionsList

    for (requisitionData in requisitions.requisitionsList) {
      val requisition = Requisition.getDefaultInstance()
        .newBuilderForType()
        .apply { TextFormat.Parser.newBuilder().build().merge(requisitionData, this) }
        .build() as Requisition

      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()

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

      // Get EventGroups for this requisition from different EDPs
      val eventGroupDataList = requisitionSpec.events.eventGroupsList.map {
        val eventGroupKey =
          EventGroupKey.fromName(it.key)
            ?: throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Invalid EventGroup resource name ${it.key}",
            )
        val ds = it.value.collectionInterval.toRange().toString()
        EventGroupData(eventGroupKey, ds, "")
      }

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
            requisitionSpec.nonce,
            eventGroupDataList,
            directProtocolConfig,
            selectReachAndFrequencyNoiseMechanism(directNoiseMechanismOptions),
          )
        } else if (measurementSpec.hasDuration()) {
          // TODO
        } else if (measurementSpec.hasImpression()) {
          // TODO
        } else {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.SPEC_INVALID,
            "Measurement type not supported for direct fulfillment.",
          )
        }
      } else if (protocols.any { it.hasLiquidLegionsV2() }) {
        // TODO

      } else if  (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
        // TODO

      } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
        // TODO

      } else {
        throw Exception("Protocol not supported")
      }

      // TODO: Save result to kingdom for backward compatibility
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
    nonce: Long,
    eventGroupDataList: Iterable<EventGroupData>,
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism
  ) {
    // TODO: charge privacy budget

    logger.info("Calculating direct reach and frequency...")
    val samples = sampleVids(eventGroupDataList, measurementSpec.vidSamplingInterval)
    val measurementResult = buildDirectMeasurementResult(
      directProtocolConfig,
      directNoiseMechanism,
      measurementSpec,
      samples,
    )

    fulfillDirectMeasurement(requisition, measurementSpec, nonce, measurementResult)
  }

  protected suspend fun fulfillDirectMeasurement(
    requisition: Requisition,
    measurementSpec: MeasurementSpec,
    nonce: Long,
    measurementResult: Measurement.Result,
  ) {
    RequisitionFulfiller.logger.log(Level.INFO, "Direct MeasurementSpec:\n$measurementSpec")
    RequisitionFulfiller.logger.log(Level.INFO, "Direct MeasurementResult:\n$measurementResult")

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
      signResult(measurementResult, dataProviderSigningKeyHandle!!)
    val encryptedResult: EncryptedMessage =
      encryptResult(signedResult, measurementEncryptionPublicKey)

    try {
      requisitionsStub.fulfillDirectRequisition(
        fulfillDirectRequisitionRequest {
          name = requisition.name
          this.encryptedResult = encryptedResult
          this.nonce = nonce
          this.certificate = dataProviderCertificateKey!!.toName()
        }
      )
    } catch (e: StatusException) {
      throw Exception("Error fulfilling direct requisition ${requisition.name}", e)
    }
  }

  private suspend fun sampleVids(
    eventGroupDataList: Iterable<EventGroupData>,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
  ): Iterable<Long> {
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width
    require(vidSamplingIntervalWidth > 0 && vidSamplingIntervalWidth <= 1.0) {
      "Invalid vidSamplingIntervalWidth $vidSamplingIntervalWidth"
    }
    require(
      vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalStart + vidSamplingIntervalWidth <= 1
    ) {
      "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
        "$vidSamplingIntervalWidth"
    }

    val labelledImpressions = eventGroupDataList.map {
      val eventGroupId = it.eventGroupKey.eventGroupId
      val ds = it.ds
      val prefix = it.prefix
      val blobKey = "/$prefix/ds/$ds/event-group-id/$eventGroupId/metadata"

      val encryptedDekBlob = storageClient.getBlob(blobKey)!!
      val encryptedDekData =
        encryptedDekBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
      val encryptedDek = EncryptedDEK.getDefaultInstance()
        .newBuilderForType()
        .apply { TextFormat.Parser.newBuilder().build().merge(encryptedDekData, this) }
        .build() as EncryptedDEK

      // Get blobKey used to retrieve merged sharded impressions
      // "/$prefix/ds/$ds/event-group-id/$eventGroupId/sharded-impressions
      val shardedStorageBlobKey = storageClient.getBlob(encryptedDek.blobKey)!!
        .read().reduce { acc, byteString -> acc.concat(byteString)}.toStringUtf8()

      shardedStorageClient.getBlob(shardedStorageBlobKey)!!
        .read().toList().map {
          LabelerOutput.getDefaultInstance()
            .newBuilderForType()
            .apply { TextFormat.Parser.newBuilder().build().merge(it.toStringUtf8(), this) }
            .build() as LabelerOutput
        }
    }.flatten()

    return labelledImpressions
      .map { it.peopleList.map { jt -> jt.virtualPersonId } }
      .flatten()
      .filter { vid ->
        sampler.vidIsInSamplingBucket(
          vid,
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth,
        )
      }
  }

  /**
   * Build [Measurement.Result] of the measurement type specified in [MeasurementSpec].
   *
   * @param measurementSpec Measurement spec.
   * @param samples sampled events.
   * @return [Measurement.Result].
   */
  private fun buildDirectMeasurementResult(
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism,
    measurementSpec: MeasurementSpec,
    samples: Iterable<Long>,
  ): Measurement.Result {
    val protocolConfigNoiseMechanism = if (directNoiseMechanism == DirectNoiseMechanism.NONE) {
      NoiseMechanism.NONE
    } else if (directNoiseMechanism == DirectNoiseMechanism.CONTINUOUS_LAPLACE) {
      NoiseMechanism.CONTINUOUS_LAPLACE
    } else {
      NoiseMechanism.CONTINUOUS_GAUSSIAN
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
          reach = reach {
            value = scaledNoisedReachValue
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
          }
          frequency = frequency {
            relativeFrequencyDistribution.putAll(noisedFrequencyMap.mapKeys { it.key.toLong() })
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicDistribution = DeterministicDistribution.getDefaultInstance()
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.IMPRESSION -> {
        if (!directProtocolConfig.hasDeterministicCount()) {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for impression computation.",
          )
        }

        val sampledImpressionCount =
          MeasurementResults.computeImpression(samples, measurementSpec.impression.maximumFrequencyPerUser)

        logger.info("Adding $directNoiseMechanism publisher noise to impression...")
        val sampledNoisedImpressionCount =
          addImpressionPublisherNoise(
            sampledImpressionCount,
            measurementSpec.impression,
            directNoiseMechanism,
          )
        val scaledNoisedImpressionCount =
          (sampledNoisedImpressionCount / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          impression = impression {
            value = scaledNoisedImpressionCount
            noiseMechanism = protocolConfigNoiseMechanism
            deterministicCount = DeterministicCount.getDefaultInstance()
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.DURATION -> {
        MeasurementKt.result {
          watchDuration = watchDuration {
            value = duration {
              // Use a value based on the externalDataProviderId since it's a known value the
              // MeasurementConsumerSimulator can verify.
              seconds = 0 // TODO: How do you get Duration???
            }
            noiseMechanism = protocolConfigNoiseMechanism
            customDirectMethodology = customDirectMethodology {
              variance = variance { scalar = 0.0 }
            }
          }
        }
      }
      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        error("Measurement type not supported.")
      }
      MeasurementSpec.MeasurementTypeCase.REACH -> {
        if (!directProtocolConfig.hasDeterministicCountDistinct()) {
          throw RequisitionRefusalException(
            Requisition.Refusal.Justification.DECLINED,
            "No valid methodologies for direct reach computation.",
          )
        }

        val sampledReachValue = MeasurementResults.computeReach(samples)

        logger.info("Adding $directNoiseMechanism publisher noise to direct reach for reach-only")
        val sampledNoisedReachValue =
          addReachPublisherNoise(
            sampledReachValue,
            measurementSpec.reach.privacyParams,
            directNoiseMechanism,
          )
        val scaledNoisedReachValue =
          (sampledNoisedReachValue / measurementSpec.vidSamplingInterval.width).toLong()

        MeasurementKt.result {
          reach = reach {
            value = scaledNoisedReachValue
            this.noiseMechanism = protocolConfigNoiseMechanism
            deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
          }
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
   * Add publisher noise to calculated impression.
   *
   * @param impressionValue Impression value.
   * @param impressionMeasurementSpec Measurement spec of impression.
   * @param directNoiseMechanism Selected noise mechanism for impression.
   * @return Noised non-negative impression value.
   */
  private fun addImpressionPublisherNoise(
    impressionValue: Long,
    impressionMeasurementSpec: MeasurementSpec.Impression,
    directNoiseMechanism: DirectNoiseMechanism,
  ): Long {
    val noiser: AbstractNoiser =
      getPublisherNoiser(impressionMeasurementSpec.privacyParams, directNoiseMechanism, random)
    // Noise needs to be scaled by maximumFrequencyPerUser.
    val noise = noiser.sample() * impressionMeasurementSpec.maximumFrequencyPerUser
    return max(0L, impressionValue + noise.roundToInt())
  }

//  private suspend fun getLabelledImpressionsBlobKey(requisitionSpec: RequisitionSpec)
//  private suspend fun getShardedLabelledImpressions(requisitionSpec: RequisitionSpec) {
//
//  }



//  private suspend fun getRequisition(resourceName: String): Requisition {
//    // TODO: Will we get the requisition from storage or kingdom? Because currently the list of requisition
//    // paths is stored in storage. It doesnt make sense to store the list and the individual requisitions
//    // in storage. So I need to either store the list of type Requisition in the path from TriggeredApp.path
//    // and just use those, or
//    val requisitionBlob = storageClient.getBlob(resourceName)!!
//    val requisitionData = requisitionBlob
//      .read()
//      .reduce { acc, byteString -> acc.concat(byteString) }
//      .toStringUtf8()
//    return Requisition.getDefaultInstance()
//      .newBuilderForType()
//      .apply { TextFormat.Parser.newBuilder().build().merge(requisitionData, this) }
//      .build() as Requisition
//  }

  fun generateSketch(
    metaLaberOutputList: List<MetaLabelerOutput>,
    sketchConfig: SketchConfig,
  ): Sketch {
    logger.info("Generating Sketch...")
    val sketch = SketchProtos.toAnySketch(sketchConfig)
    metaLaberOutputList.map {metaLaberOutput ->
      metaLaberOutput.labelerOutput.peopleList.map {
        sketch.insert(it.virtualPersonId, mapOf("frequency" to 1L))
      }
    }
    return SketchProtos.fromAnySketch(sketch, sketchConfig)
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val VID_SAMPLER_HASH_FUNCTION: HashFunction = Hashing.farmHashFingerprint64()
    val sampler = VidSampler(VID_SAMPLER_HASH_FUNCTION)
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
