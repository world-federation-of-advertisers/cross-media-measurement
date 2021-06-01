// Copyright 2020 The Cross-Media Measurement Authors
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
 * This sanitizes [schemaDefinitionLines] for the Spanner emulator by removing comments and
 * constraints.
 */
fun createDatabase(
  spannerInstance: Instance,
  schemaDefinitionLines: Sequence<String>,
  databaseName: String
): Database =
  spannerInstance.createDatabase(databaseName, toDdlStatements(schemaDefinitionLines)).get()

/** Converts schema definition lines to data definition language (DDL) statements. */
private fun toDdlStatements(sdl: Sequence<String>): List<String> {
  return sdl
    // Strip comments.
    .map { it.replace("""--.*$""".toRegex(), "") }
    // Remove blank lines.
    .filter { it.isNotBlank() }
    // Join lines to single string.
    .joinToString("\n")
    // Split into statements.
    .split(';')
    // Remove blank statements.
    .filter { it.isNotBlank() }
}
