package org.wfanet.measurement.storage.testing

import com.google.common.truth.Truth.assertThat
import kotlin.random.Random
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.Test
import org.wfanet.measurement.storage.BYTES_PER_MIB
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.createBlob
import org.wfanet.measurement.storage.toByteArray

/** Abstract base class for testing implementations of [StorageClient]. */
abstract class AbstractStorageClientTest<T : StorageClient<*>> {
  protected lateinit var storageClient: T

  @Test fun `createBlob creates new blob`() = runBlocking {
    val blobKey = "new-blob"

    val blob = storageClient.createBlob(blobKey, testBlobContent)

    // Read it back and verify that they are the same.
    val readBytes = blob.read().toByteArray()
    assertThat(readBytes).hasLength(testBlobContent.size)
    assertThat(readBytes).isEqualTo(testBlobContent)
  }

  @Test fun `getBlob returns null for non-existant blob`() {
    val blobKey = "non-existant-blob"
    assertThat(storageClient.getBlob(blobKey)).isNull()
  }

  @Test fun `getBlob returns readable Blob`() = runBlocking {
    val blobKey = "blob-to-get"
    storageClient.createBlob(blobKey, testBlobContent)

    val blob = storageClient.getBlob(blobKey)

    val readBytes = assertNotNull(blob).read().toByteArray()
    assertThat(readBytes).hasLength(testBlobContent.size)
    assertThat(readBytes).isEqualTo(testBlobContent)
  }

  @Test fun `Blob delete deletes blob`() = runBlocking {
    val blobKey = "blob-to-delete"
    val blob = storageClient.createBlob(blobKey, testBlobContent)

    blob.delete()

    assertThat(storageClient.getBlob(blobKey)).isNull()
  }

  companion object {
    private val random = Random.Default
    protected lateinit var testBlobContent: ByteArray

    @BeforeClass
    @JvmStatic
    fun generateTestBytes() {
      testBlobContent = random.nextBytes(random.nextInt(BYTES_PER_MIB * 3, BYTES_PER_MIB * 4))
    }
  }
}
