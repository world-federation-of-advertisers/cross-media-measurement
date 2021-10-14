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

package org.wfanet.panelmatch.client.privatemembership

import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.toByteString

/** Reads a blob into a PCollection. */
class ReadAsSingletonPCollection(
  private val blobKey: String,
  private val storageFactory: StorageFactory
) : PTransform<PBegin, PCollection<ByteString>>() {
  override fun expand(input: PBegin): PCollection<ByteString> {
    return input.apply(
      "Read $blobKey",
      Create.ofProvider(ReadFileProvider(blobKey, storageFactory), ByteStringCoder.of())
    )
  }
}

private class ReadFileProvider(
  private val blobKey: String,
  private val storageFactory: StorageFactory
) : ValueProvider<ByteString> {
  override fun get(): ByteString {
    return runBlocking(Dispatchers.IO) {
      requireNotNull(storageFactory.build().getBlob(blobKey)?.toByteString())
    }
  }

  override fun isAccessible(): Boolean = true
}
