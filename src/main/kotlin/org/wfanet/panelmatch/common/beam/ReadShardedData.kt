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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.toByteString

/** Reads each file mentioned in [fileSpec] as a [ByteString]. */
class ReadShardedData<T : Message>(
  private val prototype: T,
  private val fileSpec: String,
  private val storageFactory: StorageFactory
) : PTransform<PBegin, PCollection<T>>() {
  override fun expand(input: PBegin): PCollection<T> {
    val fileNames: PCollection<String> =
      input.pipeline.apply("Create FileSpec", Create.of(fileSpec)).flatMap("Make BlobKeys") {
        ShardedFileName(fileSpec).fileNames.asIterable()
      }

    // GroupByKey prevents fusing `mapValues` since the previous ParDo has high fan-out.
    return fileNames
      .keyBy("Prevent fusion: Key") { it }
      .groupByKey("Prevent Fusion/Group")
      .map("Prevent Fusion/Unkey+Ungroup") { it.value.single() }
      .apply("Read Each Blob", ParDo.of(ReadBlobFn(prototype, storageFactory)))
      .setCoder(ProtoCoder.of(prototype.javaClass))
  }
}

private class ReadBlobFn<T : MessageLite>(
  private val prototype: T,
  private val storageFactory: StorageFactory
) : DoFn<String, T>() {
  private val metricsNamespace = "ReadShardedData"
  private val fileSizeDistribution = Metrics.distribution(metricsNamespace, "file-sizes")
  private val itemSizeDistribution = Metrics.distribution(metricsNamespace, "item-sizes")
  private val elementCountDistribution = Metrics.distribution(metricsNamespace, "element-counts")

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val blob = storageFactory.build().getBlob(context.element()) ?: return
    val bytes = runBlocking(Dispatchers.IO) { blob.toByteString() }

    fileSizeDistribution.update(bytes.size().toLong())

    var elements = 0L

    for (message in bytes.parseDelimitedMessages(prototype)) {
      itemSizeDistribution.update(message.serializedSize.toLong())
      context.output(message)
      elements++
    }

    elementCountDistribution.update(elements)
  }
}
