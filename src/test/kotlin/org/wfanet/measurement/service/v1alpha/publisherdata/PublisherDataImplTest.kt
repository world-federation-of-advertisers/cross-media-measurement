package org.wfanet.measurement.service.v1alpha.publisherdata

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpc

@RunWith(JUnit4::class)
class PublisherDataImplTest {
  @Rule
  @JvmField
  val grpcCleanup = GrpcCleanupRule()

  var blockingStub: PublisherDataGrpc.PublisherDataBlockingStub? = null

  @Before
  fun setup() {
    val serverName = InProcessServerBuilder.generateName()
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(PublisherDataImpl())
        .build()
        .start()
    )
    blockingStub =
      PublisherDataGrpc.newBlockingStub(
        grpcCleanup.register(
          InProcessChannelBuilder.forName(
            serverName
          ).directExecutor().build()
        )
      )
  }

  @Test
  fun `getCombinedPublicKey throws an invalid argument exception when id is empty`() {
    val e = assertFailsWith(StatusRuntimeException::class) {
      blockingStub!!.getCombinedPublicKey(GetCombinedPublicKeyRequest.getDefaultInstance())
    }

    assertThat(e.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
  }

  @Test
  fun `getCombinedPublicKey returns the id received in the request`() {
    val fakeKey = "Ceci n'est pas une clé"

    val response =
      blockingStub!!.getCombinedPublicKey(
        GetCombinedPublicKeyRequest.newBuilder()
          .setKey(
            CombinedPublicKey.Key.newBuilder()
              .setCombinedPublicKeyId(fakeKey)
          )
          .build()
      )

    ProtoTruth.assertThat(response)
      .isEqualTo(
        CombinedPublicKey.newBuilder()
          .setKey(
            CombinedPublicKey.Key.newBuilder()
              .setCombinedPublicKeyId(fakeKey)
          )
          .build()
      )
  }
}
