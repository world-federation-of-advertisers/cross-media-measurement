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

package org.wfanet.panelmatch.common.certificates.gcloud

import com.google.cloud.security.privateca.v1.PublicKey as CloudPublicKey
import com.google.cloud.security.privateca.v1.PublicKey.KeyFormat
import com.google.protobuf.ByteString
import java.security.PublicKey
import org.wfanet.measurement.common.crypto.PemWriter

/** Converts a java.security.PublicKey to com.google.cloud.security.privateca.v1.PublicKey */
fun PublicKey.toGCloudPublicKey(): CloudPublicKey {
  val publicKeyBytes =
    ByteString.newOutput().use { output ->
      val writer = PemWriter(output)
      writer.write(this)
      output.toByteString()
    }
  return CloudPublicKey.newBuilder().setKey(publicKeyBytes).setFormat(KeyFormat.PEM).build()
}
