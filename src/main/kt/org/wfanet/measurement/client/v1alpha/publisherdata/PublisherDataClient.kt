package org.wfanet.measurement.client.v1alpha.publisherdata

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpc
import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

class PublisherDataClient : Closeable {
  private val channel: ManagedChannel
  private val blockingStub: PublisherDataGrpc.PublisherDataBlockingStub
  private val host: String = "localhost"
  private val port: Int = 31125

  init {
    channel =
      ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    blockingStub = PublisherDataGrpc.newBlockingStub(channel)
  }

  override fun close() {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }

  fun getCombinedPublicKey() {
    logger.info("Sending request to GetCombinedPublicKey...")
    try {
      val response =
        blockingStub.getCombinedPublicKey(GetCombinedPublicKeyRequest.getDefaultInstance())
      logger.info("Response: ${response}")
    } catch (e: StatusRuntimeException) {
      logger.warning("RPC failed: ${e.status}")
    }
  }

  companion object {
    private val logger = Logger.getLogger(PublisherDataClient::class.java.name)
  }
}
