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
import com.google.protobuf.CodedOutputStream
import java.io.OutputStream
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.security.PrivateKey
import java.security.Signature
import java.security.cert.X509Certificate
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.WriteFilesResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.panelmatch.common.toByteString

internal class WriteSignedFiles(
  private val fileSpec: String,
  private val privateKey: PrivateKey,
  private val certificate: X509Certificate,
) : PTransform<PCollection<ByteString>, WriteFilesResult<Void>>() {
  private val fileSpecBreakdown = FileSpecBreakdown(fileSpec)

  override fun expand(input: PCollection<ByteString>): WriteFilesResult<Void> {
    return input.apply(
      FileIO.write<ByteString>()
        .to(fileSpecBreakdown.directoryUri)
        .withPrefix(fileSpecBreakdown.prefix)
        .withNumShards(fileSpecBreakdown.shardCount)
        .via(SignedFileSink(fileSpec, privateKey, certificate))
    )
  }
}

private class SignedFileSink(
  fileSpec: String,
  private val privateKey: PrivateKey,
  private val certificate: X509Certificate
) : FileIO.Sink<ByteString> {
  private lateinit var outputStream: OutputStream
  private lateinit var codedOutput: CodedOutputStream
  private lateinit var signer: Signature
  private val fileName = fileSpec.substringAfterLast('/')

  override fun open(channel: WritableByteChannel) {
    outputStream = Channels.newOutputStream(channel)
    codedOutput = CodedOutputStream.newInstance(outputStream)
    signer = Signature.getInstance(certificate.sigAlgName, jceProvider)
    signer.initSign(privateKey)
  }

  override fun write(element: ByteString) {
    codedOutput.writeBoolNoTag(true)
    codedOutput.writeBytesNoTag(element)
    signer.update(element.asReadOnlyByteBuffer())
  }

  override fun flush() {
    codedOutput.writeBoolNoTag(false)

    val uniqueIdBytes = fileName.toByteString()
    codedOutput.writeBytesNoTag(uniqueIdBytes)
    signer.update(uniqueIdBytes.asReadOnlyByteBuffer())

    codedOutput.writeByteArrayNoTag(signer.sign())
  }
}
