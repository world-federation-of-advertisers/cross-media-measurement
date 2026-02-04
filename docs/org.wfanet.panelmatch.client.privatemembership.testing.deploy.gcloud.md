# org.wfanet.panelmatch.client.privatemembership.testing.deploy.gcloud

## Overview
This package provides an Apache Beam Dataflow pipeline for end-to-end testing of Private Membership cryptographic operations. The pipeline demonstrates query creation, database population, query evaluation, decryption, and result persistence to BigQuery using Google Cloud Platform infrastructure.

## Components

### DataflowRoundTripTestMain
Main executable that orchestrates a complete Private Membership round-trip workflow on Google Cloud Dataflow.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| main | `args: Array<String>` | `Unit` | Executes end-to-end Private Membership test pipeline |
| makeOptions | `args: Array<String>` | `Options` | Parses and validates pipeline configuration |
| makeFakeUserDataPayload | `suffix: String` | `ByteString` | Generates test payload with prefix and suffix |
| toBigQuery | `outputTable: String, tableSchema: TableSchema` | `Unit` | Writes PCollection to BigQuery table |

### Options
Pipeline configuration interface extending DataflowPipelineOptions.

| Property | Type | Description |
|----------|------|-------------|
| resultsOutputTable | `String` | BigQuery destination table in format `<project>:<dataset>.<table>` |

### AsymmetricKeysValueProvider
Lazy value provider for generating Private Membership cryptographic key pairs.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| get | - | `AsymmetricKeyPair` | Returns lazily-generated key pair |
| isAccessible | - | `Boolean` | Indicates value is always accessible |

## Pipeline Architecture

### Data Flow
1. **Key Generation**: Creates asymmetric cryptographic keys using JNI Private Membership library
2. **Query Creation**: Generates lookup queries distributed across shards
3. **Query Encryption**: Encrypts queries using RLWE homomorphic encryption
4. **Database Population**: Creates test database entries with random lookup keys
5. **Query Evaluation**: Evaluates encrypted queries against database
6. **Result Decryption**: Decrypts encrypted query results
7. **BigQuery Export**: Writes decrypted results to BigQuery

### Configuration Parameters
| Parameter | Value | Purpose |
|-----------|-------|---------|
| SHARD_COUNT | 100 | Number of database shards |
| BUCKETS_PER_SHARD_COUNT | 2047 | Buckets per shard for hash distribution |
| QUERIES_PER_SHARD_COUNT | 16 | Queries allocated per shard |

## Dependencies
- `org.apache.beam` - Dataflow pipeline orchestration and BigQuery I/O
- `com.google.privatemembership.batch` - Private Membership protocol implementation
- `org.wfanet.panelmatch.client.privatemembership` - Query creation and evaluation logic
- `org.wfanet.panelmatch.common.beam` - Beam utility transforms (map, flatMap, parDo)
- `org.wfanet.panelmatch.common.crypto` - Cryptographic key pair abstractions
- `com.google.protobuf` - Protocol buffer serialization

## Usage Example
```kotlin
// Command-line execution
fun main(args: Array<String>) {
  // args example: ["--resultsOutputTable=project:dataset.table", "--runner=DataflowRunner"]
  val options = makeOptions(args)
  FileSystems.setDefaultPipelineOptions(options)

  val pipeline = Pipeline.create(options)
  // Pipeline construction and execution...
  val result = pipeline.run()
  check(result.waitUntilFinish() == PipelineResult.State.DONE)
}
```

## Output Schema
Results are written to BigQuery with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| QueryId | INT64 | Unique identifier for the query |
| ResultIndex | INT64 | Index of result within query matches |
| Result | STRING | Decrypted user data payload |

## Implementation Notes
- Uses JNI bindings (`JniPrivateMembership`, `JniPrivateMembershipCryptor`, `JniQueryEvaluator`) for cryptographic operations
- Employs padding nonces to ensure security of Private Membership protocol
- Generates synthetic test data with 333-character prefix for realistic payload sizes
- Supports running on Google Cloud Dataflow with configurable parallelism
- Creates BigQuery table if not exists, truncates existing data on write
