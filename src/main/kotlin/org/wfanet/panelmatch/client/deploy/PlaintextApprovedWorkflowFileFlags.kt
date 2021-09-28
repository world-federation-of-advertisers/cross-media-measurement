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

import com.google.common.io.BaseEncoding
import java.nio.file.Paths
import java.security.MessageDigest
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidationKey
import org.wfanet.panelmatch.common.PlaintextFileSecretSet
import org.wfanet.panelmatch.common.SecretSet
import picocli.CommandLine

object PlaintextApprovedWorkflowFileFlags {
  @CommandLine.Option(
    names = ["--approved_workflow_file"],
    description =
      [
        "File where each line is an approved Recurring Exchange ID and a SHA256-hashed " +
          "serialized ExchangeWorkflow separated by a comma"],
    required = true,
    converter = [SecretSetConverter::class],
  )
  lateinit var secretSet: SecretSet<ValidationKey>
    private set
}

private class SecretSetConverter : CommandLine.ITypeConverter<SecretSet<ValidationKey>> {
  override fun convert(value: String): SecretSet<ValidationKey> {
    return PlaintextFileSecretSet(Paths.get(value), ::translateValidationKey)
  }
}

private fun translateValidationKey(validationKey: ValidationKey): String {
  val sha256MessageDigest = MessageDigest.getInstance("SHA-256")
  val hash = sha256MessageDigest.digest(validationKey.serializedExchangeWork.toByteArray())
  val hexHash = BaseEncoding.base16().encode(hash)
  return "${validationKey.recurringExchangeId},$hexHash"
}
