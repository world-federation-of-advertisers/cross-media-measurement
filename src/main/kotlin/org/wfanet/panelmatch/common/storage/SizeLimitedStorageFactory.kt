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

package org.wfanet.panelmatch.common.storage

import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.storage.StorageClient

/** [StorageFactory] for [SizeLimitedStorageClient]. */
private class SizeLimitedStorageFactory(
  private val sizeLimitBytes: Long,
  private val delegate: StorageFactory,
) : StorageFactory {
  override fun build(): StorageClient {
    return SizeLimitedStorageClient(sizeLimitBytes, delegate.build())
  }

  override fun build(options: PipelineOptions?): StorageClient {
    return SizeLimitedStorageClient(sizeLimitBytes, delegate.build(options))
  }
}

/** Wraps a [StorageFactory] to limit blob sizes. */
fun StorageFactory.withBlobSizeLimit(sizeLimitBytes: Long): StorageFactory {
  return SizeLimitedStorageFactory(sizeLimitBytes, this)
}
