// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common.server

import com.google.crypto.tink.Aead
import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import java.io.File
import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupSync
import picocli.CommandLine

/** Explicit operator grants and a shared keyset for opaque sync tokens. */
class EventGroupSyncFlags {
  @CommandLine.Option(
    names = ["--event-group-sync-operator"],
    description =
      [
        "Authenticated principal resource name allowed to sync every EventGroup. Repeat for each operator."
      ],
  )
  private var operatorNames: List<String> = emptyList()

  @CommandLine.Option(
    names = ["--event-group-sync-token-keyset-file"],
    description =
      ["Binary Tink AEAD keyset. Share across replicas; retain old keys while tokens remain valid."],
  )
  private var keysetFile: File? = null

  fun loadOptions(clock: Clock): EventGroupSync.Options? {
    val file = keysetFile
    if (file == null) {
      require(operatorNames.isEmpty()) { "Sync operators require a token keyset" }
      return null
    }
    return EventGroupSync.Options(
      operatorNames.toSet(),
      TinkProtoKeysetFormat.parseKeyset(
          file.readBytes(),
          InsecureSecretKeyAccess.get(),
          RegistryConfiguration.get(),
        )
        .getPrimitive(RegistryConfiguration.get(), Aead::class.java),
      Duration.ofMinutes(15),
      Duration.ofDays(30),
      clock,
    )
  }

  companion object {
    init {
      AeadConfig.register()
    }
  }
}
