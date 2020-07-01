package org.wfanet.measurement.db.gcp.testing

import com.google.cloud.spanner.Database
import com.google.cloud.spanner.Instance

/**
 * Creates a [Database] with name [databaseName] in [spannerInstance] using the schema [ddl].
 *
 * This sanitizes [ddl] for the Spanner emulator by removing comments and constraints.
 */
fun createDatabase(
  spannerInstance: Instance,
  ddl: String,
  databaseName: String
): Database =
  spannerInstance.createDatabase(databaseName, sanitizeSdl(ddl)).get()

private fun sanitizeSdl(sdl: String): List<String> =
  sdl.split('\n')
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
