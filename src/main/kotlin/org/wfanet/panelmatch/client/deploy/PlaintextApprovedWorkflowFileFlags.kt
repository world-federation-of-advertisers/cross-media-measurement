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

package org.wfanet.panelmatch.client.deploy

import java.nio.file.Paths
import org.wfanet.panelmatch.common.secrets.CsvSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import picocli.CommandLine

object PlaintextApprovedWorkflowFileFlags {
  @CommandLine.Option(
    names = ["--approved_workflow_file"],
    description =
      [
        "File where each line is an approved Recurring Exchange ID and a Base64-encoded " +
          "serialized ExchangeWorkflow separated by a comma"],
    required = true,
    converter = [SecretMapConverter::class],
  )
  lateinit var approvedExchangeWorkflows: SecretMap
    private set
}

private class SecretMapConverter : CommandLine.ITypeConverter<SecretMap> {
  override fun convert(value: String): SecretMap {
    return CsvSecretMap(Paths.get(value))
  }
}
