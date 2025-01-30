package org.wfanet.measurement.securecomputation.requisitions

import com.google.cloud.storage.StorageOptions
import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine

class RequisitionFetcherRunner: Runnable {
  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  private lateinit var target: String

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
    [
      "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
      "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
    ],
    required = true,
  )
  private lateinit var certHost: String

  @CommandLine.Option(
    names = ["--gcs-project-id"],
    description =
    [
      "Project ID for the GCS instance where the new requisitions will be stored."
    ],
    required = true,
  )
  private lateinit var gcsProjectId: String

  @CommandLine.Option(
    names = ["--gcs-bucket"],
    description =
    [
      "Name of the bucket within the GCS instance where the new requisitions will be stored"
    ],
    required = true,
  )
  private lateinit var gcsBucket: String

  @CommandLine.Option(
    names = ["--data-provider-resource-name"],
    description =
    [
      "The public API resource name of the data provider for which requisitions will be fetched."
    ],
    required = true,
  )
  private lateinit var dataProviderResourceName: String

  @CommandLine.Option(
    names = ["--throttler-minimum-interval"],
    description = ["Minimum throttle interval"],
    defaultValue = "300s", // 5 minutes
  )
  private lateinit var throttlerMinimumInterval: Duration

  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set



  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )

    val publicChannel = buildMutualTlsChannel(
      target,
      clientCerts,
      certHost,
    )

    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    val gcsStorageClient = GcsStorageClient(
      StorageOptions.newBuilder().setProjectId(gcsProjectId).build().service,
      gcsBucket
    )

    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerMinimumInterval)

    RequisitionFetcher(requisitionsStub, gcsStorageClient, dataProviderResourceName, gcsBucket, throttler)
  }
}
