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

package org.wfanet.panelmatch.client.exchangetasks

import com.google.protobuf.ByteString
import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient.VerifiedBlob
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.beam.SignedFiles

/** Base class for Apache Beam-running ExchangeTasks. */
abstract class ApacheBeamTask : ExchangeTask {
  protected abstract val uriPrefix: String
  protected abstract val privateKey: PrivateKey
  protected abstract val localCertificate: X509Certificate

  protected val pipeline: Pipeline = Pipeline.create()

  protected suspend fun readFromManifest(
    manifest: VerifiedBlob,
    certificate: X509Certificate
  ): PCollection<ByteString> {
    val shardedFileName = manifest.toStringUtf8()
    return pipeline.apply(
      "Read $shardedFileName",
      SignedFiles.read("$uriPrefix/$shardedFileName", certificate)
    )
  }

  protected fun readFileAsSingletonPCollection(
    fileName: String,
    certificate: X509Certificate
  ): PCollection<ByteString> {
    return pipeline.apply("Read $fileName", SignedFiles.read("$uriPrefix/$fileName", certificate))
  }

  // TODO: consider also adding a helper to write non-sharded files.
  protected fun PCollection<ByteString>.write(shardedFileName: ShardedFileName) {
    apply(
      "Write ${shardedFileName.spec}",
      SignedFiles.write("$uriPrefix/${shardedFileName.spec}", privateKey, localCertificate)
    )
  }
}
