package org.wfanet.measurement.securecomputation.teeapps.resultsfulfillment

import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.unpack
import io.grpc.StatusException
import java.security.GeneralSecurityException
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.reduce
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPublicKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.dataprovider.RequisitionFulfiller
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.SketchGenerator
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig.TriggeredApp
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.TeeAppConfig


class ResultFulfiller(
  private val gcsStorageClient: GcsStorageClient,
  private val certificatesStub: CertificatesCoroutineStub,
  val privateEncryptionKey: PrivateKeyHandle,
  private val eventQuery: EventQuery<Message>,
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<TriggeredApp>,
): BaseTeeApplication<TriggeredApp>(subscriptionId,queueSubscriber,parser) {

  override suspend fun runWork(message: TriggeredApp) {
    val teeAppConfig = message.config.unpack(TeeAppConfig::class.java)
    assert(teeAppConfig.workTypeCase == TeeAppConfig.WorkTypeCase.REACH_AND_FREQUENCY_CONFIG)
    val reachAndFrequencyConfig = teeAppConfig.reachAndFrequencyConfig
    for (requisitionName in reachAndFrequencyConfig.requisitionNamesList) {
      val requisition = getRequisition(requisitionName)
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
            val directProtocol =
              DirectProtocol(
                directProtocolConfig,
                selectReachAndFrequencyNoiseMechanism(directNoiseMechanismOptions),
              )
            fulfillDirectReachAndFrequencyMeasurement(
              requisition,
              measurementSpec,
              requisitionSpec.nonce,
              eventGroupSpecs,
              directProtocol,
            )
          } else if (measurementSpec.hasDuration()) {
            val directProtocol =
              DirectProtocol(
                directProtocolConfig,
                selectImpressionNoiseMechanism(directNoiseMechanismOptions),
              )
            fulfillDurationMeasurement(
              requisition,
              requisitionSpec,
              measurementSpec,
              eventGroupSpecs,
              directProtocol,
            )
          } else if (measurementSpec.hasImpression()) {
            val directProtocol =
              DirectProtocol(
                directProtocolConfig,
                selectWatchDurationNoiseMechanism(directNoiseMechanismOptions),
              )
            fulfillImpressionMeasurement(
              requisition,
              requisitionSpec,
              measurementSpec,
              eventGroupSpecs,
              directProtocol,
            )
          } else {
            throw Exception(
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

  suspend fun getRequisition(resourceName: String): Requisition {
    val requisitionBlob = gcsStorageClient.getBlob(resourceName)!!
    val requisitionData = requisitionBlob
      .read()
      .reduce { acc, byteString -> acc.concat(byteString) }
      .toStringUtf8()
    return Requisition.getDefaultInstance()
      .newBuilderForType()
      .apply { TextFormat.Parser.newBuilder().build().merge(requisitionData, this) }
      .build() as Requisition
  }

  suspend fun getVidModel() {

  }

  suspend fun generateSketch(
    sketchConfig: SketchConfig,
    measurementSpec: MeasurementSpec,
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  ): Sketch {
    logger.info("Generating Sketch...")
    val sketch =
      SketchGenerator(eventQuery, sketchConfig, measurementSpec.vidSamplingInterval)
        .generate(eventGroupSpecs)

    logger.log(Level.INFO) { "SketchConfig:\n${sketch.config}" }
    logger.log(Level.INFO) { "Registers Size:\n${sketch.registersList.size}" }

    return sketch
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
  }

}
