# org.wfanet.measurement.duchy.deploy.common.job.mill.liquidlegionsv2

## Overview
This package provides job implementations for processing Multi-Party Computation (MPC) protocols using the Liquid Legions v2 sketch aggregation algorithm. It includes configurable mill jobs that claim and process duchy computations for both reach-frequency and reach-only measurement types, with support for forwarded storage backends and flexible command-line configuration.

## Components

### LiquidLegionsV2MillJob
Abstract base class for Liquid Legions v2 mill jobs that processes claimed computation work using the Liquid Legions v2 protocol.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| run | `storageClient: StorageClient` | `Unit` | Initializes channels, clients, and mill instance, then processes claimed and available work |

**Key Responsibilities:**
- Initializes duchy identity from configuration flags
- Establishes mutual TLS channels to computation services, system API, and other duchies
- Creates computation data clients for storage and service communication
- Loads consent signaling certificates and signing keys
- Instantiates appropriate mill type based on computation type (reach-frequency or reach-only)
- Processes claimed computation work and continuously claims new work until exhausted

**Supported Computation Types:**
- `LIQUID_LEGIONS_SKETCH_AGGREGATION_V2` - Creates `ReachFrequencyLiquidLegionsV2Mill`
- `REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2` - Creates `ReachOnlyLiquidLegionsV2Mill`

### ForwardedStorageLiquidLegionsV2MillJob
Concrete implementation of `LiquidLegionsV2MillJob` with forwarded storage backend support.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| run | - | `Unit` | Constructs forwarded storage client and delegates to parent run method |
| main | `args: Array<String>` | `Unit` | Entry point for command-line execution |

**Annotations:**
- `@CommandLine.Command` - Configures Picocli command-line interface with help options and default value display

### LiquidLegionsV2MillFlags
Configuration flag aggregator for Liquid Legions v2 mill jobs, extending `MillFlags` with protocol-specific options.

| Property | Type | Description |
|----------|------|-------------|
| duchy | `CommonDuchyFlags` | Common duchy configuration |
| duchyInfoFlags | `DuchyInfoFlags` | Multi-duchy identity information |
| systemApiFlags | `SystemApiFlags` | System API service connection flags |
| computationsServiceFlags | `ComputationsServiceFlags` | Computations service connection flags |
| claimedComputationFlags | `ClaimedComputationFlags` | Flags for claimed computation identification |
| parallelism | `Int` | Maximum thread count for cryptographic operations (default: 1) |

**Flag Categories:**
- Duchy configuration and identity
- Service endpoints and TLS settings
- Computation identification and versioning
- Cryptographic operation parallelism

## Dependencies

### Internal Dependencies
- `org.wfanet.measurement.common.crypto` - Certificate and key management, signing operations
- `org.wfanet.measurement.common.grpc` - Mutual TLS channel construction, deadline management
- `org.wfanet.measurement.common.identity` - Duchy identity initialization and context propagation
- `org.wfanet.measurement.duchy.db.computation` - Computation data client abstraction
- `org.wfanet.measurement.duchy.mill` - Base mill functionality and certificate handling
- `org.wfanet.measurement.duchy.mill.liquidlegionsv2` - Protocol-specific mill implementations
- `org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto` - JNI-based cryptographic workers
- `org.wfanet.measurement.internal.duchy` - Internal duchy service stubs (Computations, ComputationStats)
- `org.wfanet.measurement.storage` - Storage client abstraction
- `org.wfanet.measurement.storage.forwarded` - Forwarded storage backend implementation
- `org.wfanet.measurement.system.v1alpha` - System API service stubs (Computations, Participants, LogEntries, Control)

### External Dependencies
- `com.google.protobuf` - Protocol buffer support for serialization
- `io.grpc` - gRPC channel and communication framework
- `kotlinx.coroutines` - Asynchronous coroutine support for processing loop
- `picocli` - Command-line interface parsing and configuration

## Usage Example

```kotlin
// Command-line execution with required flags
fun main(args: Array<String>) {
  commandLineMain(ForwardedStorageLiquidLegionsV2MillJob(), args)
}

// Typical command-line invocation
// ForwardedStorageLiquidLegionsV2MillJob \
//   --duchy-name=worker1 \
//   --computations-service-target=localhost:8080 \
//   --system-api-target=localhost:9090 \
//   --tls-cert-file=/path/to/cert.pem \
//   --tls-private-key-file=/path/to/key.pem \
//   --tls-cert-collection-file=/path/to/trusted.pem \
//   --cs-certificate-der-file=/path/to/cs-cert.der \
//   --cs-private-key-der-file=/path/to/cs-key.der \
//   --claimed-computation-type=LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 \
//   --claimed-global-computation-id=123 \
//   --parallelism=4
```

## Architectural Context

### Mill Processing Flow
1. **Initialization** - Duchy info, TLS certificates, service channels created
2. **Client Setup** - gRPC stubs configured for internal and system services
3. **Mill Creation** - Appropriate mill instance selected based on computation type
4. **Work Processing** - Claimed computation processed first, then continuous work claiming loop
5. **Termination** - Processing continues until work exhausted or coroutine cancelled

### Multi-Duchy Coordination
The job establishes `ComputationControlCoroutineStub` connections to all other duchies in the system (excluding itself), enabling multi-party computation coordination. Each duchy participates in the protocol by exchanging encrypted sketch data through these channels.

### Cryptographic Components
- **Consent Signaling Certificate** - Loaded from DER file, used for measurement consent verification
- **Signing Key** - Private key for signing computation messages
- **Trusted Certificates** - CA bundle for verifying other duchy certificates
- **JNI Crypto Workers** - Native library implementations for performance-critical cryptographic operations

## Class Diagram

```mermaid
classDiagram
    class LiquidLegionsV2MillJob {
        <<abstract>>
        #flags: LiquidLegionsV2MillFlags
        #run(storageClient: StorageClient)
    }

    class ForwardedStorageLiquidLegionsV2MillJob {
        -forwardedStorageFlags: ForwardedStorageFromFlags.Flags
        +run()
        +main(args: Array~String~)
    }

    class LiquidLegionsV2MillFlags {
        +duchy: CommonDuchyFlags
        +duchyInfoFlags: DuchyInfoFlags
        +systemApiFlags: SystemApiFlags
        +computationsServiceFlags: ComputationsServiceFlags
        +claimedComputationFlags: ClaimedComputationFlags
        +parallelism: Int
    }

    class MillFlags {
        <<external>>
    }

    class Runnable {
        <<interface>>
    }

    ForwardedStorageLiquidLegionsV2MillJob --|> LiquidLegionsV2MillJob
    LiquidLegionsV2MillJob ..|> Runnable
    LiquidLegionsV2MillFlags --|> MillFlags
    LiquidLegionsV2MillJob --> LiquidLegionsV2MillFlags
    ForwardedStorageLiquidLegionsV2MillJob --> ForwardedStorageFromFlags
