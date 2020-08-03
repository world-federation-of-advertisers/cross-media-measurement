package org.wfanet.measurement.storage.gcs

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.wfanet.measurement.storage.BYTES_PER_MIB
import org.wfanet.measurement.storage.StorageClient

/** Size of byte buffer used to read/write blobs from the storage system. */
private const val BYTE_BUFFER_SIZE = BYTES_PER_MIB * 1

/** Google Cloud Storage implementation of [StorageClient] for a single bucket. */
class CloudStorageClient(
  private val cloudStorage: Storage,
  private val bucketName: String,
  override val coroutineContext: CoroutineContext = EmptyCoroutineContext
) : StorageClient<CloudStorageClient.ClientBlob>, CoroutineScope {

  override fun createBlobAsync(blobKey: String, content: Flow<Byte>): Deferred<ClientBlob> {
    val blob = cloudStorage.create(BlobInfo.newBuilder(bucketName, blobKey).build())
    val result = CompletableDeferred<ClientBlob>()

    launch(Dispatchers.IO) {
      blob.writer().use { byteChannel ->
        byteChannel.collectFrom(content)
      }
      result.complete(ClientBlob(blob))
    }

    return result
  }

  override fun getBlob(blobKey: String): ClientBlob? {
    val blob: Blob? = cloudStorage.get(bucketName, blobKey)
    return if (blob == null) null else ClientBlob(blob)
  }

  /** [StorageClient.Blob] implementation for [CloudStorageClient]. */
  inner class ClientBlob(private val blob: Blob) : StorageClient.Blob {
    @OptIn(ExperimentalCoroutinesApi::class) // For `onCompletion`.
    override fun read(): Flow<Byte> {
      val byteChannel = blob.reader()
      return byteChannel.emitAll().flowOn(Dispatchers.IO).onCompletion { byteChannel.close() }
    }

    override fun delete() {
      check(blob.delete()) { "Failed to delete blob ${blob.blobId}" }
    }
  }
}

/** Flushes the byte buffer to the specified byte channel. */
private fun ByteBuffer.flush(writeChannel: WritableByteChannel) {
  flip()
  while (hasRemaining()) {
    writeChannel.write(this)
  }
  clear()
}

/** Flushes the byte buffer as a flow. */
private fun ByteBuffer.flushAsFlow() = flow<Byte> {
  flip()
  while (hasRemaining()) {
    emit(get())
  }
  clear()
}

/** Emits all of the bytes from this byte channel. */
private fun ReadableByteChannel.emitAll() = flow<Byte> {
  val buffer = ByteBuffer.allocate(BYTE_BUFFER_SIZE)
  @Suppress("BlockingMethodInNonBlockingContext") // Should be running in Dispatchers.IO.
  while (read(buffer) >= 0) {
    emitAll(buffer.flushAsFlow())
  }
}

/** Collects the bytes from the specified flow into this byte channel. */
@OptIn(ExperimentalCoroutinesApi::class) // For `onCompletion`.
private suspend fun WritableByteChannel.collectFrom(content: Flow<Byte>) {
  val byteChannel = this
  val buffer = ByteBuffer.allocate(BYTE_BUFFER_SIZE)

  content.buffer().onEach { byte ->
    buffer.put(byte)
    if (!buffer.hasRemaining()) {
      buffer.flush(byteChannel)
    }
  }.onCompletion { cause ->
    // Flush whatever is left in the buffer.
    if (cause == null && buffer.position() > 0) {
      buffer.flush(byteChannel)
    }
  }.collect()
}
