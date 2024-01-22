// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.deploy

import kotlin.properties.Delegates
import org.wfanet.panelmatch.common.certificates.CertificateAuthority.Context
import picocli.CommandLine.Option

class CertificateAuthorityFlags {
  @Option(
    names = ["--x509-common-name"],
    description = ["X509 Certificate Common Name (CN)"],
    required = true,
  )
  private lateinit var commonName: String

  @Option(
    names = ["--x509-organization"],
    description = ["X509 Certificate Organization (O)"],
    required = true,
  )
  private lateinit var organization: String

  @Option(
    names = ["--x509-dns-name"],
    description = ["X509 Certificate Alt Subject DNS name"],
    required = true,
  )
  private lateinit var dnsName: String

  @set:Option(
    names = ["--x509-valid-days"],
    description = ["X509 Certificate lifespan"],
    required = true,
  )
  private var validDays: Int by Delegates.notNull()

  val context: Context by lazy { Context(commonName, organization, dnsName, validDays) }
}
