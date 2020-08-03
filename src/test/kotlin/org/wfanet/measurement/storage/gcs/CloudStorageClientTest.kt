package org.wfanet.measurement.storage.gcs

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import org.junit.Before
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.storage.testing.AbstractStorageClientTest

@RunWith(JUnit4::class)
class CloudStorageClientTest : AbstractStorageClientTest<CloudStorageClient>() {
  @Before
  fun initClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = CloudStorageClient(storage, BUCKET)
  }

  companion object {
    private const val BUCKET = "test-bucket"
  }
}
