// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.storage

import java.nio.ByteBuffer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion

const val BYTES_PER_MIB = 1024 * 1024
const val DEFAULT_FLOW_BUFFER_SIZE = 4096 // 4 KiB

/**
 * Interface for blob/object storage operations.
 */
interface StorageClient<out B : StorageClient.Blob> {
  /** Creates a blob with the specified key and content. */
  suspend fun createBlob(blobKey: String, content: Flow<ByteBuffer>): B

  /** Returns a [Blob] for the specified key, or `null` if it cannot be found. */
  fun getBlob(blobKey: String): B?

  /** Reference to a blob in a storage system. */
  interface Blob {
    /** Size of the blob in bytes. */
    val size: Long

    /** Returns a [Flow] for the blob content. */
    fun read(flowBufferSize: Int = DEFAULT_FLOW_BUFFER_SIZE): Flow<ByteBuffer>

    /** Deletes the blob. */
    fun delete()
  }
}

/** Creates a blob with the specified key and content. */
suspend fun <B : StorageClient.Blob> StorageClient<B>.createBlob(
  blobKey: String,
  content: ByteArray
): B = createBlob(blobKey, content.asBufferedFlow())

/** Reads all of this [Blob] content into a [ByteArray]. */
suspend fun StorageClient.Blob.readAll(): ByteArray {
  check(size <= Int.MAX_VALUE) { "Blob cannot fit in a single byte array" }

  val buffer = ByteBuffer.allocate(size.toInt())
  read().collect { value ->
    buffer.put(value)
  }
  return buffer.array()
}

/**
 * Bulk copies the remaining bytes in the source buffer into this one until
 * either the source buffer has no more remaining bytes or the limit of this
 * buffer is reached.
 *
 * Both buffers will have their positions incremented by the number of bytes copied.
 */
private fun ByteBuffer.putUntilLimit(src: ByteBuffer): ByteBuffer {
  if (remaining() >= src.remaining()) {
    put(src)
  } else {
    val sliced = src.slice()
    sliced.limit(remaining())
    put(sliced)
    src.position(src.position() + sliced.limit())
  }

  return this // For chaining.
}

/** Creates a flow that produces [ByteBuffer] values from this byte buffer. */
fun ByteBuffer.asBufferedFlow(flowBufferSize: Int = DEFAULT_FLOW_BUFFER_SIZE) = flow<ByteBuffer> {
  require(flowBufferSize > 0)

  while (hasRemaining()) {
    val buffer = ByteBuffer.allocate(flowBufferSize)
    buffer.putUntilLimit(this@asBufferedFlow).flip()
    emit(buffer)
  }
}

/** Creates a flow that produces [ByteBuffer] values from this byte array. */
fun ByteArray.asBufferedFlow(flowBufferSize: Int = DEFAULT_FLOW_BUFFER_SIZE) =
  ByteBuffer.wrap(this).asBufferedFlow(flowBufferSize)

/** Creates a flow that produces [ByteBuffer] values of the specified size from this flow. */
@OptIn(ExperimentalCoroutinesApi::class) // For `onCompletion`.
fun Flow<ByteBuffer>.asBufferedFlow(flowBufferSize: Int) = flow<ByteBuffer> {
  require(flowBufferSize > 0)
  val collector = this

  var outputBuffer = ByteBuffer.allocate(flowBufferSize)
  onCompletion { cause ->
    // Emit a final buffer with whatever is left.
    if (cause == null && outputBuffer.position() > 0) {
      outputBuffer.flip()
      collector.emit(outputBuffer)
    }
  }.collect { inputBuffer ->
    while (inputBuffer.hasRemaining()) {
      outputBuffer.putUntilLimit(inputBuffer)

      // If output buffer is full, emit it and allocate a new one.
      if (!outputBuffer.hasRemaining()) {
        outputBuffer.flip()
        collector.emit(outputBuffer)
        outputBuffer = ByteBuffer.allocate(flowBufferSize)
      }
    }
  }
}
