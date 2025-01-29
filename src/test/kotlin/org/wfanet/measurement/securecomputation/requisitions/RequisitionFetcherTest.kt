package org.wfanet.measurement.securecomputation.requisitions

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.Storage
import com.google.protobuf.Timestamp
import java.io.BufferedWriter
import java.io.PrintWriter
import java.io.StringWriter
import java.net.HttpURLConnection
import java.net.URL
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.*
import org.mockito.kotlin.whenever
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.kingdomConfig
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.storageConfig
import org.wfanet.measurement.storage.testing.InMemoryStorageClient

@RunWith(JUnit4::class)
class RequisitionFetcherTest {

  @Test
  fun `fetch new requisitions`() {
    val request: HttpRequest = mock()
    val response: HttpResponse = mock()

    //@TODO(jojijacob): Implement test

    val kingdomConfig = kingdomConfig {
      publicApiTarget = "target"
      publicApiCertHost = "cert-host"
      certCollectionPath = "collection-path"
      apiAuthenticationKey = "authentication-key"
      dataProvider = "dataprovider"
    }

    val storageConfig = storageConfig {
      project = "project-id"
      bucket = "bucket"
      blobUri = "https://storage.googleapis.com/bucket/blob"
      lastUpdate = Timestamp.getDefaultInstance()
    }
    val fetcher = RequisitionFetcher(kingdomConfig, storageConfig)
    fetcher.service(request, response)
  }

}
