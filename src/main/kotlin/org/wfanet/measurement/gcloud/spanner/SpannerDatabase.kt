// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.gcloud.spanner

import com.google.cloud.spanner.Database
import com.google.cloud.spanner.Instance

/**
 * Creates a [Database] with name [databaseName] in [spannerInstance] using the
 * [schemaDefinitionLines].
 *
 * This sanitizes [schemaDefinitionLines] for the Spanner emulator by removing
 * comments and constraints.
 */
fun createDatabase(
  spannerInstance: Instance,
  schemaDefinitionLines: Sequence<String>,
  databaseName: String
): Database =
  spannerInstance.createDatabase(databaseName, sanitizeSdl(schemaDefinitionLines)).get()

private fun sanitizeSdl(sdl: Sequence<String>): List<String> {
  return sdl
    // Replace comments and references to foreign keys from schema file.
    .map { it.replace("""(--|CONSTRAINT|FOREIGN|REFERENCES).*$""".toRegex(), "") }
    // Delete blank lines
    .filter { it.isNotBlank() }
    // Rejoin to single sting
    .joinToString("\n")
    // and split into operations
    .split(';')
    // Removing any blank operations
    .filter { it.isNotBlank() }
}
