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

import com.google.protobuf.Message
import kotlin.math.abs
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.PInput
import org.apache.beam.sdk.values.POutput
import org.apache.beam.sdk.values.PValue
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.toDelimitedByteString

/** Writes input messages into blobs. */
class WriteShardedData<T : Message>(
  private val fileSpec: String,
  private val storageFactory: StorageFactory
) : PTransform<PCollection<T>, WriteShardedData.WriteResult>() {

  /** [POutput] holding filenames written. */
  class WriteResult(private val fileNames: PCollection<String>) : POutput {
    override fun getPipeline(): Pipeline = fileNames.pipeline

    override fun expand(): Map<TupleTag<*>, PValue> {
      return mapOf(tag to fileNames)
    }

    override fun finishSpecifyingOutput(
      transformName: String,
      input: PInput,
      transform: PTransform<*, *>
    ) {}

    companion object {
      private val tag = TupleTag<String>()
    }
  }

  override fun expand(input: PCollection<T>): WriteResult {
    val shardedFileName = ShardedFileName(fileSpec)
    val shardCount = shardedFileName.shardCount

    val groupedData: PCollection<KV<Int, Iterable<T>>> =
      input
        .keyBy("Key by Blob") { it.assignToShard(shardCount) }
        .apply("Group by Blob", GroupByKey.create())

    val missingFiles: PCollection<KV<Int, Iterable<T>>> =
      groupedData.keys("Present Keys").combineIntoList("Combine Present Keys").parDo {
        val keySet = it.toHashSet()
        for (i in 0 until shardCount) {
          if (i !in keySet) {
            yield(kvOf(i, emptyList<T>().asIterable()))
          }
        }
      }

    missingFiles.coder = groupedData.coder

    val filesWritten =
      PCollectionList.of(groupedData)
        .and(missingFiles)
        .apply("Flatten", Flatten.pCollections())
        .apply("Write $fileSpec", ParDo.of(WriteFilesFn(fileSpec, storageFactory)))

    return WriteResult(filesWritten)
  }
}

private class WriteFilesFn<T : Message>(
  private val fileSpec: String,
  private val storageFactory: StorageFactory
) : DoFn<KV<Int, Iterable<@JvmWildcard T>>, String>() {

  @ProcessElement
  fun processElement(context: ProcessContext) {
    val kv = context.element()
    val blobKey = ShardedFileName(fileSpec).fileNameForShard(kv.key)
    val storageClient = storageFactory.build()
    val messageFlow = kv.value.asFlow().map { it.toDelimitedByteString() }

    runBlocking(Dispatchers.IO) { storageClient.writeBlob(blobKey, messageFlow) }

    context.output(blobKey)
  }
}

/** Returns an [Int] shard index for [this]. */
fun Any.assignToShard(shardCount: Int): Int {
  // The conversion to Long avoids the special case where abs(Int.MIN_VALUE) returns Int.MIN_VALUE.
  val longShard = abs(hashCode().toLong()) % shardCount
  return longShard.toInt()
}
