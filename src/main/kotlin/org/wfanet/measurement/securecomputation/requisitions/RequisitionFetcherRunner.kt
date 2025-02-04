package org.wfanet.measurement.securecomputation.requisitions

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import java.io.File
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.toByteArray
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

class RequisitionFetcherRunner: HttpFunction {
  override fun service(request: HttpRequest?, response: HttpResponse?) {
    val clientCerts = runBlocking {
      getClientCerts()
    }

    val publicChannel = buildMutualTlsChannel(
      System.getenv("TARGET"),
      clientCerts,
      System.getenv("CERT_HOST"),
    )

    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    val requisitionsStorageClient = GcsStorageClient(
      StorageOptions.newBuilder().setProjectId(System.getenv("REQUISITIONS_GCS_PROJECT_ID")).build().service,
      System.getenv("REQUISITIONS_GCS_BUCKET")
    )

    val fetcher = RequisitionFetcher(requisitionsStub, requisitionsStorageClient, System.getenv("DATAPROVIDER_NAME"), System.getenv("GCS_BUCKET"))
    runBlocking {
      fetcher.executeRequisitionFetchingWorkflow()
    }
  }

  private suspend fun getClientCerts(): SigningCerts {
    val authenticationStorageClient = GcsStorageClient(
      StorageOptions.newBuilder().setProjectId(System.getenv("AUTHENTICATION_GCS_PROJECT_ID")).build().service,
      System.getenv("AUTHENTICATION_GCS_BUCKET")
    )

    val certBlob = authenticationStorageClient.getBlob("gs://${System.getenv("AUTHENTICATION_GCS_BUCKET")}/${System.getenv("CERT_FILE_PATH")}")
    val privateKeyBlob = authenticationStorageClient.getBlob("gs://${System.getenv("AUTHENTICATION_GCS_BUCKET")}/${System.getenv("PRIVATE_KEY_FILE_PATH")}")
    val certCollectionBlob = authenticationStorageClient.getBlob("gs://${System.getenv("AUTHENTICATION_GCS_BUCKET")}/${System.getenv("CERT_COLLECTION_FILE_PATH")}")


    val certFile = File.createTempFile("cert", ".pem")
    certFile.writeBytes(checkNotNull(certBlob).read().toByteArray())

    val privateKeyFile = File.createTempFile("private_key", ".key")
    privateKeyFile.writeBytes(checkNotNull(privateKeyBlob).read().toByteArray())

    val certCollectionFile = File.createTempFile("cert_collection", ".pem")
    certCollectionFile.writeBytes(checkNotNull(certCollectionBlob).read().toByteArray())


    return SigningCerts.fromPemFiles(
      certificateFile = certFile,
      privateKeyFile = privateKeyFile,
      trustedCertCollectionFile = certCollectionFile,
    )
  }
}
