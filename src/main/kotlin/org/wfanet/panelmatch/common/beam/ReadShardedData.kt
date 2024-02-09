// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common.beam

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.MessageLite
import java.io.InputStream
import kotlin.random.Random
import kotlin.random.nextInt
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.StorageClient.Blob
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.newInputStream

/** Reads each file mentioned in [fileSpec] as a [ByteString]. */
class ReadShardedData<T : Message>(
  private val prototype: T,
  private val fileSpec: String,
  private val storageFactory: StorageFactory,
  private val maxConcurrentCopies: Int = 200
) : PTransform<PBegin, PCollection<T>>() {
  override fun expand(input: PBegin): PCollection<T> {
    val fileNames: PCollection<String> =
      input.pipeline.apply("Create FileSpec", Create.of(fileSpec)).flatMap("Make BlobKeys") {
        ShardedFileName(fileSpec).fileNames.asIterable()
      }

    return fileNames
      .breakFusion("Break Fusion Before ReadBlobFn")
      .map("Map to random bucket") { kvOf(Random.nextInt(1, maxConcurrentCopies), it) }
      .groupByKey("Group filenames")
      .parDo(ReadBlobFn(prototype, storageFactory), name = "Read Each Blob")
      .setCoder(ProtoCoder.of(prototype.javaClass))
  }
}

private class ReadBlobFn<T : MessageLite>(
  private val prototype: T,
  private val storageFactory: StorageFactory,
) : DoFn<KV<Int, Iterable<@JvmWildcard String>>, T>() {
  private val metricsNamespace = "ReadShardedData"
  private val itemSizeDistribution = Metrics.distribution(metricsNamespace, "item-sizes")
  private val elementCountDistribution = Metrics.distribution(metricsNamespace, "element-counts")

  @Setup
  fun setup() {
    println("running setup")
    storageClient = storageFactory.build()
  }

  @ProcessElement
  fun processElement(context: ProcessContext) =
    runBlocking(Dispatchers.IO) {
      val pipelineOptions = context.pipelineOptions
      val contextElement = context.element().value
      semaphore.acquire()
      println("lock acquired, ${semaphore.availablePermits} remain.")
      try {
        for (blobKey in contextElement) {
          println("getting blob: $blobKey")
          try {
            val blob: Blob =
              storageClient.getBlob(blobKey) ?: return@runBlocking
            println("blob gotten: $blobKey")
            val inputStream: InputStream = blob.newInputStream(this)

            var elements = 0L

            for (message in inputStream.parseDelimitedMessages(prototype)) {
              itemSizeDistribution.update(message.serializedSize.toLong())
              context.output(message)
              elements++
            }

            elementCountDistribution.update(elements)
            println("blobKey $blobKey has been read with $elements elements.")
          } catch (e: Exception) {
            println("error reading blob for $blobKey")
            throw e
          }
        }
      } catch(e: Exception) {
        throw e
      } finally {
        semaphore.release()
        println("releasing lock for $contextElement, ${semaphore.availablePermits} remain.")
      }
    }

  companion object {
    lateinit var storageClient: StorageClient
    val semaphore = Semaphore(10)
  }
}
