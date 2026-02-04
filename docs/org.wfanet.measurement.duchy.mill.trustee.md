# org.wfanet.measurement.duchy.mill.trustee

## Overview
This package implements the TrusTEE (Trusted Execution Environment) protocol for privacy-preserving measurement computations within the Duchy mill architecture. It provides stateful processing of encrypted frequency vectors from multiple data providers to compute reach and frequency distributions with differential privacy guarantees, operating within a secure execution environment with KMS-based cryptographic operations.

## Components

### TrusTeeMill
Main mill implementation for TrusTEE protocol computations, extending MillBase to orchestrate the complete computation lifecycle from initialization through result generation.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| processComputationImpl | `token: ComputationToken` | `Unit` | Executes computation stage workflow for TrusTEE |
| initializationPhase | `token: ComputationToken` | `ComputationToken` | Sends requisition params to Kingdom and transitions to WAIT_TO_START |
| computingPhase | `token: ComputationToken` | `ComputationToken` | Aggregates frequency vectors and computes final result |
| TrusTeeDetails.toTrusTeeParams | (extension) | `TrusTeeParams` | Converts protocol computation details to processor parameters |
| getKmsClient | `kmsClientFactory: KmsClientFactory<GCloudWifCredentials>`, `protocol: RequisitionProtocol.TrusTee` | `KmsClient` | Creates KMS client with workload identity federation |
| getDekKeysetHandle | `kmsClient: KmsClient`, `protocol: RequisitionProtocol.TrusTee` | `KeysetHandle` | Decrypts data encryption key using KMS KEK |
| getRequisitionData | `requisition: RequisitionMetadata` | `ByteString` | Reads requisition blob from storage |
| decryptRequisitionData | `dek: KeysetHandle`, `data: ByteString` | `ByteArray` | Decrypts frequency vector using streaming AEAD |
| toIntArray | `bytes: ByteArray` | `IntArray` | Converts byte array to int array |

**Constructor Parameters:**
- `millId: String` - Unique mill identifier
- `duchyId: String` - Duchy identifier
- `signingKey: SigningKeyHandle` - Signing key for authentication
- `consentSignalCert: Certificate` - Certificate for consent signal validation
- `dataClients: ComputationDataClients` - Storage layer access
- `systemComputationParticipantsClient: ComputationParticipantsCoroutineStub` - Kingdom participants client
- `systemComputationsClient: ComputationsCoroutineStub` - Kingdom computations client
- `systemComputationLogEntriesClient: ComputationLogEntriesCoroutineStub` - Computation logging client
- `computationStatsClient: ComputationStatsCoroutineStub` - Computation statistics client
- `workLockDuration: Duration` - Duration for computation work lock
- `trusTeeProcessorFactory: TrusTeeProcessor.Factory` - Factory for processor instances
- `kmsClientFactory: KmsClientFactory<GCloudWifCredentials>` - Factory for KMS clients
- `attestationTokenPath: Path` - Path to attestation token file
- `requestChunkSizeBytes: Int` - Chunk size for requests (default: 32KB)
- `maximumAttempts: Int` - Maximum retry attempts (default: 10)
- `clock: Clock` - Clock for time operations (default: system UTC)

**Stage Workflow:**
1. `INITIALIZED` → Initialization phase → `WAIT_TO_START`
2. `COMPUTING` → Computing phase → `COMPLETE`

### TrusTeeProcessor
Interface defining stateful processor for aggregating frequency vectors and computing reach/frequency distributions with differential privacy.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| addFrequencyVector | `vector: ByteArray` | `Unit` | Accumulates frequency vector into internal state |
| computeResult | (none) | `ComputationResult` | Computes final reach/frequency result from aggregated data |

**Properties:**
- `trusTeeParams: TrusTeeParams` - Configuration parameters for the computation

**Nested Interface:**

#### TrusTeeProcessor.Factory
Factory interface for creating TrusTeeProcessor instances.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| create | `trusTeeParams: TrusTeeParams` | `TrusTeeProcessor` | Creates processor instance with specified parameters |

**Important Notes:**
- Processor is stateful and not thread-safe
- A single instance should be used per computation
- `addFrequencyVector` must be called for each data provider before `computeResult`
- Frequency vectors use 8-bit signed integers representing non-negative frequencies

### TrusTeeProcessorImpl
Concrete implementation of TrusTeeProcessor performing histogram-based reach and frequency computations.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| addFrequencyVector | `vector: ByteArray` | `Unit` | Validates and aggregates frequency vector with capping |
| computeResult | (none) | `ComputationResult` | Builds histogram and applies differential privacy to compute result |
| InternalDifferentialPrivacyParams.toDifferentialPrivacyParams | (extension) | `DifferentialPrivacyParams` | Converts internal DP params to computation library format |

**Constructor Parameters:**
- `trusTeeParams: TrusTeeParams` - Reach or reach-and-frequency parameters

**Internal State:**
- `aggregatedFrequencyVector: IntArray` - Lazily initialized on first vector addition
- `maxFrequency: Int` - Maximum frequency value (1 for reach-only, 2-127 for reach-and-frequency)
- `vidSamplingIntervalWidth: Float` - VID sampling interval (must be > 0.0 and ≤ 1.0)

**Companion Object:**
- Implements `TrusTeeProcessor.Factory` as `Factory`

## Data Structures

### TrusTeeParams
Sealed interface representing parameters for TrusTEE computations. Has two implementations:

#### TrusTeeReachParams
| Property | Type | Description |
|----------|------|-------------|
| vidSamplingIntervalWidth | `Float` | VID sampling interval width for reach estimation |
| dpParams | `DifferentialPrivacyParams` | Differential privacy parameters for reach |

#### TrusTeeReachAndFrequencyParams
| Property | Type | Description |
|----------|------|-------------|
| maximumFrequency | `Int` | Maximum frequency value to track (2-127) |
| vidSamplingIntervalWidth | `Float` | VID sampling interval width for reach estimation |
| reachDpParams | `DifferentialPrivacyParams` | Differential privacy parameters for reach |
| frequencyDpParams | `DifferentialPrivacyParams` | Differential privacy parameters for frequency distribution |

## Dependencies
- `org.wfanet.measurement.duchy.mill` - Base mill infrastructure (MillBase, Certificate)
- `org.wfanet.measurement.duchy.db.computation` - Computation data storage layer
- `org.wfanet.measurement.common.crypto` - Cryptographic utilities (SigningKeyHandle, Tink integration)
- `org.wfanet.measurement.system.v1alpha` - Kingdom API clients for computation coordination
- `org.wfanet.measurement.internal.duchy` - Internal computation protocol definitions
- `org.wfanet.measurement.computation` - Histogram and reach/frequency computation algorithms
- `org.wfanet.measurement.duchy.utils` - Computation result types
- `com.google.crypto.tink` - Cryptographic primitives (AEAD, KMS, streaming encryption)
- `com.google.protobuf` - Protocol buffer serialization

## Usage Example
```kotlin
// Create TrusTEE mill with dependencies
val mill = TrusTeeMill(
  millId = "trustee-mill-1",
  duchyId = "duchy-a",
  signingKey = signingKeyHandle,
  consentSignalCert = certificate,
  dataClients = computationDataClients,
  systemComputationParticipantsClient = participantsClient,
  systemComputationsClient = computationsClient,
  systemComputationLogEntriesClient = logEntriesClient,
  computationStatsClient = statsClient,
  workLockDuration = Duration.ofMinutes(5),
  trusTeeProcessorFactory = TrusTeeProcessorImpl.Factory,
  kmsClientFactory = kmsClientFactory,
  attestationTokenPath = Paths.get("/var/run/attestation/token")
)

// Process computation token
mill.processComputationImpl(computationToken)

// Using processor directly
val params = TrusTeeReachAndFrequencyParams(
  maximumFrequency = 10,
  vidSamplingIntervalWidth = 0.5f,
  reachDpParams = reachDpParams,
  frequencyDpParams = frequencyDpParams
)
val processor = TrusTeeProcessorImpl(params)

// Add frequency vectors from each data provider
dataProviders.forEach { provider ->
  val frequencyVector = provider.getFrequencyVector()
  processor.addFrequencyVector(frequencyVector)
}

// Compute final result
val result = processor.computeResult()
```

## Class Diagram
```mermaid
classDiagram
    class TrusTeeMill {
        -trusTeeProcessorFactory: TrusTeeProcessor.Factory
        -kmsClientFactory: KmsClientFactory
        -attestationTokenPath: Path
        +processComputationImpl(token: ComputationToken)
        -initializationPhase(token: ComputationToken) ComputationToken
        -computingPhase(token: ComputationToken) ComputationToken
        -getKmsClient(factory, protocol) KmsClient
        -getDekKeysetHandle(kmsClient, protocol) KeysetHandle
        -decryptRequisitionData(dek, data) ByteArray
    }

    class MillBase {
        <<abstract>>
        +millId: String
        +duchyId: String
        +processComputationImpl(token: ComputationToken)*
    }

    class TrusTeeProcessor {
        <<interface>>
        +trusTeeParams: TrusTeeParams
        +addFrequencyVector(vector: ByteArray)
        +computeResult() ComputationResult
    }

    class TrusTeeProcessorFactory {
        <<interface>>
        +create(trusTeeParams: TrusTeeParams) TrusTeeProcessor
    }

    class TrusTeeProcessorImpl {
        -aggregatedFrequencyVector: IntArray
        -maxFrequency: Int
        -vidSamplingIntervalWidth: Float
        +addFrequencyVector(vector: ByteArray)
        +computeResult() ComputationResult
    }

    class TrusTeeParams {
        <<sealed interface>>
    }

    class TrusTeeReachParams {
        +vidSamplingIntervalWidth: Float
        +dpParams: DifferentialPrivacyParams
    }

    class TrusTeeReachAndFrequencyParams {
        +maximumFrequency: Int
        +vidSamplingIntervalWidth: Float
        +reachDpParams: DifferentialPrivacyParams
        +frequencyDpParams: DifferentialPrivacyParams
    }

    TrusTeeMill --|> MillBase
    TrusTeeMill --> TrusTeeProcessor : creates via factory
    TrusTeeProcessorImpl ..|> TrusTeeProcessor
    TrusTeeProcessorImpl ..|> TrusTeeProcessorFactory : companion object
    TrusTeeProcessor --> TrusTeeParams
    TrusTeeProcessorImpl --> TrusTeeParams
    TrusTeeReachParams ..|> TrusTeeParams
    TrusTeeReachAndFrequencyParams ..|> TrusTeeParams
    TrusTeeProcessor +-- TrusTeeProcessorFactory
```

## Security Considerations

### Cryptographic Operations
- Uses Google Cloud KMS with Workload Identity Federation for key management
- Employs Tink library for AEAD and streaming AEAD encryption/decryption
- Data Encryption Keys (DEK) are encrypted with Key Encryption Keys (KEK) from KMS
- Attestation tokens authenticate TEE environment to cloud services

### Encryption Flow
1. Mill authenticates to KMS using attestation token via workload identity
2. Retrieves AEAD from KMS using KEK URI
3. Decrypts wrapped DEK using KMS KEK
4. Uses DEK to decrypt requisition frequency vectors
5. Processes decrypted data in secure memory
6. Results sent to Kingdom after computation

### Privacy Protection
- Differential privacy applied to all computed metrics
- VID sampling reduces correlation across measurements
- Frequency capping limits individual contribution
- No raw user-level data persists after computation
