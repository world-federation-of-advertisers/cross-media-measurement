# org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres.testing

## Overview
Test utilities package providing PostgreSQL schema file references for privacy budget management testing. This package exposes constants that reference the ledger database schema required for testing privacy budget ledger implementations backed by PostgreSQL.

## Components

### Schemata.kt

Provides schema file access constants for PostgreSQL-backed privacy budget ledger testing infrastructure.

| Constant | Type | Description |
|----------|------|-------------|
| `POSTGRES_LEDGER_SCHEMA_FILE` | `File` | Reference to the ledger.sql schema file for PostgreSQL privacy budget ledger |

#### Internal Constants

| Constant | Type | Description |
|----------|------|-------------|
| `SCHEMA_DIR` | `Path` | Path to the postgres package containing schema files |

## Data Structures

This package contains no data classes, enums, or type aliases.

## Dependencies

- `java.io.File` - File system representation
- `java.nio.file.Paths` - Path construction utilities
- `org.wfanet.measurement.common.getRuntimePath` - Resolves Bazel runtime paths to actual file system paths

## Usage Example

```kotlin
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres.testing.POSTGRES_LEDGER_SCHEMA_FILE

// Load schema for test database initialization
val schemaContent = POSTGRES_LEDGER_SCHEMA_FILE.readText()

// Execute schema creation
connection.createStatement().use { statement ->
  statement.executeUpdate(schemaContent)
}
```

## Build Configuration

This package is marked as `testonly` and has public visibility. It depends on:
- `//src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/deploy/common/postgres:ledger.sql` (data dependency)
- `@wfa_common_jvm//src/main/kotlin/org/wfanet/measurement/common` (library dependency)

## Integration

Used by test classes to access PostgreSQL schema definitions:
- `PostgresBackingStoreTest` - Tests privacy budget ledger backing store implementation
- `PrivacyBudgetPostgresSchemaTest` - Validates schema structure
- `PostgresLedgerTest` - Tests privacy budget manager ledger operations

## Schema Resolution

The `POSTGRES_LEDGER_SCHEMA_FILE` constant resolves the Bazel workspace-relative path to the actual file system location at runtime using `getRuntimePath()`. The schema file is located at:
```
wfa_measurement_system/src/main/kotlin/org/wfanet/measurement/eventdataprovider/privacybudgetmanagement/deploy/common/postgres/ledger.sql
```
