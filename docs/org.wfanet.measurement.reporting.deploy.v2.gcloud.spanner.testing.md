# org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing

## Overview
Testing utilities package for Google Cloud Spanner deployment of the reporting service v2 API. Provides schema resource path resolution for test database initialization and integration testing scenarios.

## Components

### Schemata
Singleton object providing access to reporting service Spanner database schema resources for testing purposes.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| getResourcePath | `fileName: String` | `Path` | Resolves resource path for schema file from JAR |

| Property | Type | Description |
|----------|------|-------------|
| REPORTING_CHANGELOG_PATH | `Path` | Path to reporting Spanner changelog YAML file |
| REPORTING_SPANNER_RESOURCE_PREFIX | `String` | Resource path prefix for reporting Spanner resources |

## Dependencies
- `java.nio.file.Path` - File system path representation
- `org.wfanet.measurement.common.getJarResourcePath` - Extension function for loading JAR resources

## Usage Example
```kotlin
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata

// Access the reporting database changelog path for test setup
val changelogPath = Schemata.REPORTING_CHANGELOG_PATH

// Use with Liquibase or other database migration tools
val database = Database(changelogPath)
```

## Architecture Notes
This package follows the testing utilities pattern seen across the Cross-Media Measurement project for Spanner-backed services. The `Schemata` object encapsulates resource loading logic to abstract JAR resource paths from test code, enabling consistent database schema initialization across integration tests. The private `getResourcePath` method enforces the resource naming convention (`reporting/spanner/[fileName]`) and provides clear error messages when resources are missing.
