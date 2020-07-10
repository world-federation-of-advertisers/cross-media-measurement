package org.wfanet.measurement.client.v1alpha.publisherdata

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt

fun main() = runBlocking {
  val channel =
    ManagedChannelBuilder.forAddress("localhost", 31125).usePlaintext().build()
  val stub = PublisherDataGrpcKt.PublisherDataCoroutineStub(channel)

  val response: CombinedPublicKey =
    PublisherDataClient(channel, stub).use {
      it.getCombinedPublicKey()
    }
  println("Response: $response")
}
