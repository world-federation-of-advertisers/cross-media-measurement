package org.wfanet.measurement.securecomputation.teeapps.resultsfulfillment

import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.unpack
import java.security.GeneralSecurityException
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.reduce
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.SketchConfig
import org.wfanet.anysketch.SketchProtos
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.toRange
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.SketchGenerator
import org.wfanet.measurement.loadtest.dataprovider.toSketchConfig
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EncryptedDEK
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig.TriggeredApp
import org.wfanet.measurement.securecomputation.resultsfulfillment.ShardedStorageClient
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.RequisitionsList
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.TeeAppConfig
import org.wfanet.virtualpeople.common.MetaLabelerOutput


class ResultFulfiller(
  private val storageClient: StorageClient,
  private val shardedStorageClient: ShardedStorageClient,
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

      // Get EventGroups for this requisition
      for (eventGroupEntry in requisitionSpec.events.eventGroupsList) {
        val eventGroupKey =
          EventGroupKey.fromName(eventGroupEntry.key)
            ?: throw RequisitionRefusalException(
              Requisition.Refusal.Justification.SPEC_INVALID,
              "Invalid EventGroup resource name ${eventGroupEntry.key}",
            )
        val ds = eventGroupEntry.value.collectionInterval.toRange().toString()

        // Use ds and event group id to get the blob key where the metadata of sharded impressions is stored
        val blob_key = " /{prefix}/ds/$ds/event-group-id/$eventGroupKey/metadata"
        val encryptedDekBlob = storageClient.getBlob(blob_key)!!
        val encryptedDekData =
          encryptedDekBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
        val encryptedDek = EncryptedDEK.getDefaultInstance()
          .newBuilderForType()
          .apply { TextFormat.Parser.newBuilder().build().merge(encryptedDekData, this) }
          .build() as EncryptedDEK

        // Blob key of metadata of sharded impressions. It will be a string in the format "sharded-impressions-*-of-N"
        // where N is the total number of shards
        val impressionShardsBlobKey = encryptedDek.blobKey

        // Use ShardedStorageClient to read all shards in parallel, merge them, and return the full list of MetaLabelerOutput

        val impressions = listOf<MetaLabelerOutput>()

        val protocols: List<ProtocolConfig.Protocol> = requisition.protocolConfig.protocolsList

        var sketch: Sketch
        if (protocols.any { it.hasDirect() }) {
          // TODO
        } else if (protocols.any { it.hasLiquidLegionsV2() }) {
          val llv2Protocol: ProtocolConfig.Protocol =
            requireNotNull(
              requisition.protocolConfig.protocolsList.find { protocol -> protocol.hasLiquidLegionsV2() }
            ) {
              "Protocol with LiquidLegionsV2 is missing"
            }
          val liquidLegionsV2: ProtocolConfig.LiquidLegionsV2 = llv2Protocol.liquidLegionsV2
          sketch = generateSketch(impressions, liquidLegionsV2.sketchParams.toSketchConfig())
        } else if (protocols.any { it.hasReachOnlyLiquidLegionsV2() }) {
          val protocolConfig: ProtocolConfig.ReachOnlyLiquidLegionsV2 =
            requireNotNull(
              requisition.protocolConfig.protocolsList.find { protocol ->
                protocol.hasReachOnlyLiquidLegionsV2()
              }
            ) {
              "Protocol with ReachOnlyLiquidLegionsV2 is missing"
            }
              .reachOnlyLiquidLegionsV2
          sketch = generateSketch(impressions, protocolConfig.sketchParams.toSketchConfig())
        } else if (protocols.any { it.hasHonestMajorityShareShuffle() }) {
          val protocolConfig: ProtocolConfig.HonestMajorityShareShuffle =
            requireNotNull(
              requisition.protocolConfig.protocolsList.find { protocol ->
                protocol.hasHonestMajorityShareShuffle()
              }
            ) {
              "Protocol with HonestMajorityShareShuffle is missing"
            }
              .honestMajorityShareShuffle
          // TODO
        } else {
          throw Exception("Protocol not supported")
        }
      }

      // TODO: Convert sketch to result
      // TODO: Save result to storage(or send to kingdom???)
    }
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
