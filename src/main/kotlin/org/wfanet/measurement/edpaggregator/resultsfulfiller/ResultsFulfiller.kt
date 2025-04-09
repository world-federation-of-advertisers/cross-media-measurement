package org.wfanet.measurement.edpaggregator.resultsfulfiller
import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import com.google.crypto.tink.KmsClient
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TextFormat
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
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.noiser.AbstractNoiser
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser
import org.wfanet.measurement.eventdataprovider.noiser.LaplaceNoiser
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.sampling.VidSampler

data class StorageConfig(
  val rootDirectory: File? = null,
  val projectId: String? = null,
)

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
    for (requisition in requisitions) {
      val signedRequisitionSpec: SignedMessage =
        try {
          decryptRequisitionSpec(
            requisition.encryptedRequisitionSpec,
            // TODO: CREATE THIS WITH fulfiller config
            privateEncryptionKey,
          )
        } catch (e: GeneralSecurityException) {
          throw Exception("RequisitionSpec decryption failed", e)
        }
      val requisitionSpec: RequisitionSpec = signedRequisitionSpec.unpack()
      val measurementSpec: MeasurementSpec = requisition.measurementSpec.message.unpack()

      val sampledVids = getSampledVids(
        requisitionSpec,
        measurementSpec.vidSamplingInterval
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
      } else if (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
        // TODO
      } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
        // TODO
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
    sampledVids: List<Long>,
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
  private fun buildDirectMeasurementResult(
    directProtocolConfig: ProtocolConfig.Direct,
    directNoiseMechanism: DirectNoiseMechanism,
    measurementSpec: MeasurementSpec,
    samples: Iterable<Long>,
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
          // TODO
        }
      }

      MeasurementSpec.MeasurementTypeCase.DURATION -> {
        MeasurementKt.result {
          // TODO
        }
      }

      MeasurementSpec.MeasurementTypeCase.POPULATION -> {
        MeasurementKt.result {
          // TODO
        }
      }

      MeasurementSpec.MeasurementTypeCase.REACH -> {
        MeasurementKt.result {
          // TODO
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

  fun compileProgram(
    eventFilter: RequisitionSpec.EventFilter,
    eventMessageDescriptor: Descriptor,
  ): Program {
    // EventFilters should take care of this, but checking here is an optimization that can skip
    // creation of a CEL Env.
    if (eventFilter.expression.isEmpty()) {
      return Program { TRUE_EVAL_RESULT }
    }
    return EventFilters.compileProgram(eventMessageDescriptor, eventFilter.expression)
  }

  private suspend fun getRequisitions(): List<Requisition> {
    // Gets path to list of new requisitions in blob storage
    val storageClientUri = SelectedStorageClient.parseBlobUri(requisitionsBlobUri)
    val requisitionsStorageClient = createStorageClient(storageClientUri, requisitionsStorageConfig)
    val requisitionBlob = requisitionsStorageClient.getBlob(storageClientUri.key)!!
    val requisitionData = requisitionBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
    val requisition = Requisition.getDefaultInstance()
      .newBuilderForType()
      .apply {
        TextFormat.Parser.newBuilder()
          .build()
          .merge(requisitionData, this)
      }
      .build() as Requisition

    return listOf(requisition)
  }

  private suspend fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
  ): List<Long> {
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

    return requisitionSpec.events.eventGroupsList.map { eventGroup ->
      val collectionInterval = eventGroup.value.collectionInterval
      val blobDetails = getBlobDetails(collectionInterval, eventGroup.key)

      val labeledImpressions = getLabeledImpressions(blobDetails)

      labeledImpressions.filter { labeledImpression ->
        isValidImpression(labeledImpression, collectionInterval, eventGroup, vidSamplingIntervalStart, vidSamplingIntervalWidth)
      }.map { labeledImpression ->
        labeledImpression.vid
      }
    }.flatten()
  }

  /**
   * Retrieves a list of labeled impressions from the specified storage.
   *
   * This method handles retrieving encrypted impression data from storage,
   * setting up the appropriate encryption, and parsing the raw data into
   * LabeledImpression protocol buffer messages.
   *
   * @param encryptedDek The encrypted data encryption key information
   * @param impressionsStorageConfig Storage configuration for impressions
   * @param kmsClient KMS client for handling encryption
   * @return List of parsed LabeledImpression objects
   * @throws IllegalStateException if impression data cannot be read or parsed
   */
  private suspend fun getLabeledImpressions(
    blobDetails: BlobDetails,
  ): List<LabeledImpression> {
    // Get blob URI from encrypted DEK
    val storageClientUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)

// Create and configure storage client with encryption
    val encryptedDek = blobDetails.encryptedDek
    val encryptedImpressionsClient = createStorageClient(storageClientUri, impressionsStorageConfig)
    val impressionsAeadStorageClient = encryptedImpressionsClient.withEnvelopeEncryption(
      kmsClient,
      encryptedDek.kekUri,
      encryptedDek.encryptedDek
    )

// Access blob storage
    val impressionsMesosStorage = MesosRecordIoStorageClient(impressionsAeadStorageClient)
    val impressionBlob = impressionsMesosStorage.getBlob(storageClientUri.key)
      ?: throw IllegalStateException("Could not retrieve impression blob from ${storageClientUri.key}")


    // Parse raw data into LabeledImpression objects
    val readResult = impressionBlob.read()

    val impressionRecords = readResult.toList()

    return impressionRecords.map { impressionByteString ->
      val labeledImpression = LabeledImpression.parseFrom(impressionByteString)
        ?: throw IllegalStateException("Failed to parse LabeledImpression from bytes")
      labeledImpression
    }
  }

  /**
   * Determines if a labeled impression is valid based on collection interval, filter criteria,
   * and sampling bucket.
   *
   * @param labeledImpression The impression to validate
   * @param collectionInterval Time interval for valid impressions
   * @param eventTemplateTypeUrl Type URL for the event template
   * @param typeRegistry Registry for protocol buffer types
   * @param eventGroup Event group containing filter criteria
   * @param sampler VID sampler implementation
   * @param vidSamplingIntervalStart Start of sampling interval
   * @param vidSamplingIntervalWidth Width of sampling interval
   * @return true if the impression meets all validity criteria, false otherwise
   */
  private fun isValidImpression(
    labeledImpression: LabeledImpression,
    collectionInterval: Interval,
    eventGroup: RequisitionSpec.EventGroupEntry,
    vidSamplingIntervalStart: Float,
    vidSamplingIntervalWidth: Float,
  ): Boolean {
    // Check if impression is within collection time interval
    val isInCollectionInterval =
      labeledImpression.impressionTime.toInstant() >= collectionInterval.startTime.toInstant() &&
        labeledImpression.impressionTime.toInstant() < collectionInterval.endTime.toInstant()
    // Check if VID is in sampling bucket
    val isInSamplingInterval = sampler.vidIsInSamplingBucket(
      labeledImpression.vid,
      vidSamplingIntervalStart,
      vidSamplingIntervalWidth
    )

    // Process event template filter
    val eventMessageData = labeledImpression.event!!
    val eventTemplateDescriptor = typeRegistry.getDescriptorForTypeUrl(eventMessageData.typeUrl)
    val eventMessage = DynamicMessage.parseFrom(eventTemplateDescriptor, eventMessageData.value)
    val program = compileProgram(eventGroup.value.filter, eventTemplateDescriptor)

    val passesFilter = EventFilters.matches(eventMessage, program)
    // Return true only if all conditions are met
    return isInCollectionInterval && passesFilter && isInSamplingInterval
  }
  private suspend fun getBlobDetails(collectionInterval: Interval, eventGroupId: String): BlobDetails {
    val ds = collectionInterval.startTime.toInstant().toString()
    val metadataBlobKey = "ds/$ds/event-group-id/$eventGroupId/metadata"
    val metadataBlobUri = "$labeledImpressionMetadataPrefix/$metadataBlobKey"
    val metadataStorageClientUri = SelectedStorageClient.parseBlobUri(metadataBlobUri)
    val impressionsMetadataStorageClient = createStorageClient(metadataStorageClientUri, impressionMetadataStorageConfig)
    // Get EncryptedDek message from storage using the blobKey made up of the ds and eventGroupId
    val blobDetailsBlob = impressionsMetadataStorageClient.getBlob(metadataBlobKey)!! // SELECTED STORAGE CLIENT
    val blobDetailsData =
      blobDetailsBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
    return BlobDetails.getDefaultInstance()
      .newBuilderForType()
      .apply { TextFormat.Parser.newBuilder().build().merge(blobDetailsData, this) }
      .build() as BlobDetails
  }
  /**
   * Creates a storage client for accessing blob data.
   *
   * This function constructs a [SelectedStorageClient] using the provided blob key
   * and storage configuration. It parses the blob URI and initializes the appropriate
   * client based on the storage configuration properties.
   *
   * @param blobKey The URI or path identifying the blob to access
   * @param storageConfig Configuration containing settings for storage access,
   *        including root directory and project ID
   * @return A configured [SelectedStorageClient] instance ready to access the specified blob
   */
  fun createStorageClient(blobUri: org.wfanet.measurement.storage.BlobUri, storageConfig: StorageConfig): SelectedStorageClient {
    return SelectedStorageClient(blobUri, storageConfig.rootDirectory, storageConfig.projectId)
  }
  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val VID_SAMPLER_HASH_FUNCTION: HashFunction = Hashing.farmHashFingerprint64()
    private val TRUE_EVAL_RESULT = Program.newEvalResult(BoolT.True, null)
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
