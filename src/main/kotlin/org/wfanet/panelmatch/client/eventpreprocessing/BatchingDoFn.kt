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

package org.wfanet.panelmatch.client.eventpreprocessing

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.joda.time.Instant

/**
 * Batches [T]s into MutableLists.
 *
 * The sum of [getElementByteSize] involved on each item of an output MutableList is at most
 * [maxByteSize].
 */
class BatchingDoFn<T>(
  private val maxByteSize: Int,
  private val getElementByteSize: SerializableFunction<T, Int>
) : DoFn<T, MutableList<T>>() {
  private var buffer = mutableListOf<T>()
  var size: Int = 0

  @ProcessElement
  fun process(c: ProcessContext) {
    val currElementSize: Int = getElementByteSize.apply(c.element())
    if (currElementSize >= maxByteSize) {
      c.output(mutableListOf(c.element()))
      return
    }
    if (size + currElementSize > maxByteSize) {
      c.output(buffer)
      buffer = mutableListOf()
      size = 0
    }
    buffer.add(c.element())
    size += currElementSize
  }

  @FinishBundle
  @Synchronized
  @Throws(Exception::class)
  fun FinishBundle(context: FinishBundleContext) {
    if (!buffer.isEmpty()) {
      context.output(buffer, Instant.now(), GlobalWindow.INSTANCE)
      buffer = mutableListOf()
      size = 0
    }
  }
}
