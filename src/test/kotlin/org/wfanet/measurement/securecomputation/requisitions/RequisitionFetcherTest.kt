package org.wfanet.measurement.securecomputation.requisitions

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.*
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.kingdomConfig
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.storageConfig

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
    }
    val fetcher = RequisitionFetcher(kingdomConfig, storageConfig)
    fetcher.service(request, response)
  }
}
