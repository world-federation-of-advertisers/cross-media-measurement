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
import java.security.PrivateKey
import java.security.cert.X509Certificate
import org.apache.beam.sdk.io.WriteFilesResult
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection

/** Beam PTransforms for reading and writing signed files. */
object SignedFiles {
  /**
   * Reads files using FileIO and verifies signatures appended to each file.
   *
   * TODO: load X509Certificate from KMS securely in each task.
   *
   * @param fileSpec URI specification of the form "protocol://some/path/to/files-*-of-00123".
   * @param certificate certificate to use to verify the files
   */
  fun read(
    fileSpec: String,
    certificate: X509Certificate,
  ): PTransform<PBegin, PCollection<ByteString>> {
    return ReadSignedFiles(fileSpec, certificate)
  }

  /**
   * Writes ByteStrings to sharded files. Each file is prepended with a signature.
   *
   * TODO: load PrivateKey, X509Certificate from KMS securely in each task.
   *
   * @param fileSpec URI specification of the form "protocol://some/path/to/files-*-of-00123".
   * @param privateKey the key to use to sign the data
   * @param certificate certificate to use to verify the files
   */
  fun write(
    fileSpec: String,
    privateKey: PrivateKey,
    certificate: X509Certificate,
  ): PTransform<PCollection<ByteString>, WriteFilesResult<Void>> {
    return WriteSignedFiles(fileSpec, privateKey, certificate)
  }
}
