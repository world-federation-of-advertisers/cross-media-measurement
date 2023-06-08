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

import com.google.cloud.security.privateca.v1.Certificate
import com.google.cloud.security.privateca.v1.CertificateAuthorityServiceClient
import com.google.cloud.security.privateca.v1.CreateCertificateRequest

class PrivateCaClient : CreateCertificateClient, AutoCloseable {

  private val client = CertificateAuthorityServiceClient.create()

  override suspend fun createCertificate(request: CreateCertificateRequest): Certificate {
    return client.createCertificate(request)
  }

  override fun close() {
    client.close()
  }
}
