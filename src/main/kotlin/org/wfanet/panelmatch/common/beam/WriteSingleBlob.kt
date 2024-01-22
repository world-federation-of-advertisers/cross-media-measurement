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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.apache.beam.sdk.values.PInput
import org.apache.beam.sdk.values.POutput
import org.apache.beam.sdk.values.PValue
import org.apache.beam.sdk.values.TupleTag
import org.wfanet.panelmatch.common.storage.StorageFactory

/** Writes a single input message into a blob. */
class WriteSingleBlob<T : Message>(
  private val blobKey: String,
  private val storageFactory: StorageFactory,
) : PTransform<PCollection<T>, WriteSingleBlob.WriteResult>() {

  /** [POutput] holding the single blobKey written. */
  class WriteResult(private val blobKeys: PCollection<String>) : POutput {
    override fun getPipeline(): Pipeline = blobKeys.pipeline

    override fun expand(): Map<TupleTag<*>, PValue> {
      return mapOf(tag to blobKeys)
    }

    override fun finishSpecifyingOutput(
      transformName: String,
      input: PInput,
      transform: PTransform<*, *>,
    ) {}

    companion object {
      private val tag = TupleTag<String>()
    }
  }

  override fun expand(input: PCollection<T>): WriteResult {
    val count = input.count()
    val blobKeys =
      input.apply(
        "Write $blobKey",
        ParDo.of(WriteBlobFn(blobKey, storageFactory, count)).withSideInputs(count),
      )
    return WriteResult(blobKeys)
  }
}

private class WriteBlobFn<T : Message>(
  private val blobKey: String,
  private val storageFactory: StorageFactory,
  private val count: PCollectionView<Long>,
) : DoFn<T, String>() {

  @DoFn.ProcessElement
  fun processElement(context: ProcessContext) {
    val pipelineOptions = context.getPipelineOptions()
    check(context.sideInput(count) == 1L)
    val storageClient = storageFactory.build(pipelineOptions)
    runBlocking(Dispatchers.IO) {
      storageClient.writeBlob(blobKey, context.element().toByteString())
    }
    context.output(blobKey)
  }
}
