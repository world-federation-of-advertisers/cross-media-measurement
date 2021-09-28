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
import com.google.protobuf.CodedInputStream
import java.nio.channels.Channels
import java.security.Signature
import java.security.cert.X509Certificate
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import org.wfanet.measurement.common.crypto.InvalidSignatureException
import org.wfanet.measurement.common.crypto.jceProvider

internal class ReadSignedFiles(
  private val fileSpec: String,
  private val certificate: X509Certificate
) : PTransform<PBegin, PCollection<ByteString>>() {
  override fun expand(input: PBegin): PCollection<ByteString> {
    val files = input.apply(FileIO.match().filepattern(fileSpec)).apply(FileIO.readMatches())
    val fileCount = files.count()
    return files.apply(
        "ReadSignedFiles",
        ParDo.of(ReadFilesFn(fileSpec, fileCount, certificate)).withSideInputs(fileCount)
      )
      .map { requireNotNull(it) }
  }
}

private class ReadFilesFn(
  private val fileSpec: String,
  private val fileCountView: PCollectionView<Long>,
  private val certificate: X509Certificate,
) : DoFn<ReadableFile?, ByteString?>() {
  private var fileCountIsVerified: Boolean = false
  private val fileName = fileSpec.substringAfterLast('/')

  /**
   * Throws if the actual file count (from [fileCountView]) does not match the given [fileSpec].
   *
   * Since Side Inputs are only available in [processElement], we cache in [fileCountIsVerified]
   * that we've done the verification.
   */
  private fun verifyFileCount(context: ProcessContext) {
    if (!fileCountIsVerified) {
      val fileCount = context.sideInput(fileCountView)
      require(FileSpecBreakdown(fileSpec).shardCount.toLong() == fileCount) {
        "Unexpected file count ($fileCount) for fileSpec: $fileSpec "
      }
      fileCountIsVerified = true
    }
  }

  @ProcessElement
  fun processElement(@Element readableFile: ReadableFile, context: ProcessContext) {
    verifyFileCount(context)

    Channels.newInputStream(readableFile.open()).use { stream ->
      val codedInput = CodedInputStream.newInstance(stream)

      val verifier = Signature.getInstance(certificate.sigAlgName, jceProvider)
      verifier.initVerify(certificate)

      while (codedInput.readBool()) {
        val bytes = codedInput.readBytes()
        verifier.update(bytes.asReadOnlyByteBuffer())
        context.output(bytes)
      }

      val observedFileName = codedInput.readBytes()
      verifier.update(observedFileName.asReadOnlyByteBuffer())
      require(observedFileName.toStringUtf8() == fileName) {
        "Unexpected file name: '$fileName' does not match '${observedFileName.toStringUtf8()}'"
      }

      if (!verifier.verify(codedInput.readByteArray())) {
        throw InvalidSignatureException("Signature is invalid")
      }
    }
  }
}
