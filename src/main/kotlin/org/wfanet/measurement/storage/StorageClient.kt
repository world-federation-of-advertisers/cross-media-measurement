package org.wfanet.measurement.storage

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList

const val BYTES_PER_MIB = 1024 * 1024

/**
 * Interface for blob/object storage operations.
 */
interface StorageClient<out B : StorageClient.Blob> {
  /**
   * Creates a blob with the specified key and content.
   *
   * @return a [Deferred] for the created [StorageClient.Blob], which is
   *     completed when the content has been written.
   */
  fun createBlobAsync(blobKey: String, content: Flow<Byte>): Deferred<B>

  /** Returns a [Blob] for the specified key, or `null` if it cannot be found. */
  fun getBlob(blobKey: String): B?

  /** Reference to a blob in a storage system. */
  interface Blob {
    /** Returns a [Flow] for the blob content. */
    fun read(): Flow<Byte>

    /** Deletes the blob. */
    fun delete()
  }
}

/** Creates a blob with the specified key and content. */
suspend fun <B : StorageClient.Blob> StorageClient<B>.createBlob(
  blobKey: String,
  content: ByteArray
): B {
  return createBlobAsync(blobKey, content.asIterable().asFlow()).await()
}

suspend fun Flow<Byte>.toByteArray() = toList().toByteArray()
