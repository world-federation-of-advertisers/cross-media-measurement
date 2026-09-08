/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.cloud.ReadChannel
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageException
import com.google.cloud.storage.StorageOptions
import java.io.FileNotFoundException
import java.io.IOException
import java.net.URI
import java.nio.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSInputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

/**
 * GCS Hadoop file system that recognizes the generation-qualified URI produced by
 * [generationMatchedBlobUri] and applies `ifGenerationMatch` to both metadata and content reads.
 *
 * Parquet performs several random-access range reads. The precondition is attached to the
 * [ReadChannel] itself, so an object replacement cannot make a reader silently switch generations
 * between its footer and row-group reads. GCS reports a failed precondition as HTTP 412; this class
 * turns that into an actionable [IOException]. Paths without the qualifier retain the connector's
 * normal behavior.
 */
class GenerationMatchedGoogleHadoopFileSystem : GoogleHadoopFileSystem() {
  private lateinit var storage: Storage

  override fun initialize(name: URI, configuration: Configuration) {
    super.initialize(removeGenerationUserInfo(name), configuration)
    storage =
      StorageOptions.newBuilder()
        .setProjectId(configuration.get(PROJECT_ID_PROPERTY))
        .build()
        .service
  }

  override fun getFileStatus(path: Path): FileStatus {
    val matchedPath = parseGenerationMatchedPath(path) ?: return super.getFileStatus(path)
    val blob = getGenerationMatchedBlob(storage, matchedPath)
    return FileStatus(
      requireNotNull(blob.size),
      /* isdir= */ false,
      /* block_replication= */ 1,
      defaultBlockSize,
      blob.updateTimeOffsetDateTime?.toInstant()?.toEpochMilli() ?: 0L,
      path,
    )
  }

  override fun open(path: Path, bufferSize: Int): FSDataInputStream {
    val matchedPath = parseGenerationMatchedPath(path) ?: return super.open(path, bufferSize)
    val blob = getGenerationMatchedBlob(storage, matchedPath)
    val channel =
      try {
        storage.reader(
          matchedPath.blobId,
          Storage.BlobSourceOption.generationMatch(matchedPath.generation),
        )
      } catch (e: StorageException) {
        throw storageReadException(matchedPath, e)
      }
    return FSDataInputStream(
      GenerationMatchedInputStream(channel, requireNotNull(blob.size), matchedPath)
    )
  }

  private class GenerationMatchedInputStream(
    private val channel: ReadChannel,
    private val size: Long,
    private val matchedPath: GenerationMatchedPath,
  ) : FSInputStream() {
    private var position = 0L

    override fun read(): Int {
      val byte = ByteBuffer.allocate(1)
      val count = readChannel(byte)
      return if (count < 0) -1 else byte.array()[0].toInt() and 0xff
    }

    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
      if (length == 0) return 0
      return readChannel(ByteBuffer.wrap(bytes, offset, length))
    }

    private fun readChannel(buffer: ByteBuffer): Int {
      val count =
        try {
          channel.read(buffer)
        } catch (e: StorageException) {
          throw storageReadException(matchedPath, e)
        }
      if (count > 0) position += count
      return count
    }

    override fun getPos(): Long = position

    override fun seek(newPosition: Long) {
      require(newPosition >= 0) { "Cannot seek to negative position $newPosition" }
      try {
        channel.seek(newPosition)
      } catch (e: StorageException) {
        throw storageReadException(matchedPath, e)
      }
      position = newPosition
    }

    override fun seekToNewSource(targetPosition: Long): Boolean = false

    override fun available(): Int = (size - position).coerceIn(0, Int.MAX_VALUE.toLong()).toInt()

    override fun close() {
      channel.close()
    }
  }

  companion object {
    private const val PROJECT_ID_PROPERTY = "fs.gs.project.id"
  }
}

internal data class GenerationMatchedPath(
  val cleanPath: Path,
  val blobId: BlobId,
  val generation: Long,
)

internal fun parseGenerationMatchedPath(path: Path): GenerationMatchedPath? {
  val uri = path.toUri()
  val userInfo = uri.rawUserInfo ?: return null
  if (!userInfo.startsWith(GENERATION_USER_INFO_PREFIX)) return null
  val generation =
    userInfo.removePrefix(GENERATION_USER_INFO_PREFIX).toLongOrNull()?.takeIf { it > 0 }
      ?: throw IOException("Invalid raw-impression generation in path: $path")
  val bucket =
    uri.host
      ?: uri.authority
      ?: throw IOException("Missing GCS bucket in raw-impression path: $path")
  val objectName = uri.path.removePrefix("/")
  if (objectName.isEmpty())
    throw IOException("Missing GCS object name in raw-impression path: $path")
  val cleanPath = Path(removeGenerationUserInfo(uri))
  return GenerationMatchedPath(cleanPath, BlobId.of(bucket, objectName), generation)
}

private fun removeGenerationUserInfo(uri: URI): URI {
  val userInfo = uri.rawUserInfo ?: return uri
  if (!userInfo.startsWith(GENERATION_USER_INFO_PREFIX)) return uri
  return URI.create(uri.toString().replaceFirst("://$userInfo@", "://"))
}

internal fun getGenerationMatchedBlob(storage: Storage, path: GenerationMatchedPath): Blob {
  val blob =
    try {
      storage.get(path.blobId, Storage.BlobGetOption.generationMatch(path.generation))
    } catch (e: StorageException) {
      throw storageReadException(path, e)
    } ?: throw FileNotFoundException("Raw-impression object not found: ${path.cleanPath}")
  if (blob.generation != path.generation) {
    throw generationMismatchException(path)
  }
  return blob
}

private fun storageReadException(
  path: GenerationMatchedPath,
  cause: StorageException,
): IOException =
  if (cause.code == PRECONDITION_FAILED) {
    generationMismatchException(path, cause)
  } else {
    IOException(
      "Error reading raw-impression object ${path.cleanPath} at generation ${path.generation}",
      cause,
    )
  }

private fun generationMismatchException(
  path: GenerationMatchedPath,
  cause: Throwable? = null,
): IOException =
  IOException(
    "Raw-impression object ${path.cleanPath} no longer matches its registered generation " +
      "${path.generation}. The EDP must write a new done blob to register a replacement upload.",
    cause,
  )

private const val PRECONDITION_FAILED = 412
