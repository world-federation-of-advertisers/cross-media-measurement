# org.wfanet.measurement.duchy.mill.trustee.processor

## Overview
This package provides the TrusTEE protocol processor for aggregating frequency vectors from multiple data providers to compute reach and frequency distributions. The processor supports both reach-only and reach-and-frequency measurements with differential privacy, accumulating data across multiple calls and producing final privacy-preserving aggregated results.

## Components

### TrusTeeProcessor
Stateful processor interface for TrusTEE protocol computations that aggregates frequency vectors to produce reach and frequency distributions.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| addFrequencyVector | `vector: ByteArray` | `Unit` | Accumulates frequency vector from a data provider |
| computeResult | - | `ComputationResult` | Computes final reach and frequency result from aggregated vectors |

**Property:**
- `trusTeeParams: TrusTeeParams` - Configuration parameters for the computation

**Important Notes:**
- Single instance per computation (stateful)
- Not thread-safe
- `addFrequencyVector` must be called for each data provider before `computeResult`

### TrusTeeProcessor.Factory
Factory interface for creating TrusTeeProcessor instances.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| create | `trusTeeParams: TrusTeeParams` | `TrusTeeProcessor` | Creates new processor with specified parameters |

### TrusTeeProcessorImpl
Concrete implementation of TrusTeeProcessor that maintains aggregated frequency state and performs histogram-based computations.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| addFrequencyVector | `vector: ByteArray` | `Unit` | Validates and aggregates frequency vector with coercion to max frequency |
| computeResult | - | `ComputationResult` | Builds histogram and computes differential privacy results |

**Companion Object:**
- Implements `TrusTeeProcessor.Factory` as singleton factory

**Validation Rules:**
- Maximum frequency: 2-127 for reach-and-frequency, 1 for reach-only
- VID sampling interval width: must be in range (0.0, 1.0]
- Frequency vectors: non-empty, consistent size across calls, non-negative byte values

## Data Structures

### TrusTeeParams
Sealed interface for TrusTEE computation parameters.

**Implementations:**
- `TrusTeeReachParams` - Parameters for reach-only measurements
- `TrusTeeReachAndFrequencyParams` - Parameters for reach-and-frequency measurements

### TrusTeeReachParams
| Property | Type | Description |
|----------|------|-------------|
| vidSamplingIntervalWidth | `Float` | Sampling interval width for VID sampling (0.0, 1.0] |
| dpParams | `DifferentialPrivacyParams` | Differential privacy parameters for reach |

### TrusTeeReachAndFrequencyParams
| Property | Type | Description |
|----------|------|-------------|
| maximumFrequency | `Int` | Maximum frequency cap (2-127) |
| vidSamplingIntervalWidth | `Float` | Sampling interval width for VID sampling (0.0, 1.0] |
| reachDpParams | `DifferentialPrivacyParams` | Differential privacy parameters for reach |
| frequencyDpParams | `DifferentialPrivacyParams` | Differential privacy parameters for frequency distribution |

## Dependencies
- `org.wfanet.measurement.duchy.utils` - ComputationResult, ReachResult, ReachAndFrequencyResult for output formats
- `org.wfanet.measurement.internal.duchy` - DifferentialPrivacyParams for privacy configuration
- `org.wfanet.measurement.computation` - HistogramComputations and ReachAndFrequencyComputations for aggregation algorithms
- `org.wfanet.measurement.measurementconsumer.stats` - TrusTeeMethodology for result metadata

## Usage Example
```kotlin
// Reach-only measurement
val reachParams = TrusTeeReachParams(
  vidSamplingIntervalWidth = 0.5f,
  dpParams = DifferentialPrivacyParams.newBuilder()
    .setEpsilon(0.1)
    .setDelta(1e-9)
    .build()
)

val processor = TrusTeeProcessorImpl.create(reachParams)

// Add frequency vectors from each data provider
processor.addFrequencyVector(dataProvider1Vector)
processor.addFrequencyVector(dataProvider2Vector)
processor.addFrequencyVector(dataProvider3Vector)

// Compute final result
val result = processor.computeResult()

// Reach-and-frequency measurement
val reachFreqParams = TrusTeeReachAndFrequencyParams(
  maximumFrequency = 10,
  vidSamplingIntervalWidth = 0.5f,
  reachDpParams = reachDpParams,
  frequencyDpParams = frequencyDpParams
)

val rafProcessor = TrusTeeProcessorImpl.create(reachFreqParams)
rafProcessor.addFrequencyVector(vector)
val rafResult = rafProcessor.computeResult()
```

## Class Diagram
```mermaid
classDiagram
    class TrusTeeProcessor {
        <<interface>>
        +trusTeeParams: TrusTeeParams
        +addFrequencyVector(vector: ByteArray)
        +computeResult() ComputationResult
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

    class Factory {
        <<interface>>
        +create(trusTeeParams: TrusTeeParams) TrusTeeProcessor
    }

    TrusTeeProcessor <|.. TrusTeeProcessorImpl
    TrusTeeProcessor --> TrusTeeParams
    TrusTeeProcessorImpl --> TrusTeeParams
    TrusTeeParams <|-- TrusTeeReachParams
    TrusTeeParams <|-- TrusTeeReachAndFrequencyParams
    TrusTeeProcessor +-- Factory
    Factory <|.. TrusTeeProcessorImpl : companion object
```

## Algorithm Overview

The TrusTEE processor implements a stateful aggregation workflow:

1. **Initialization**: Validates parameters (max frequency range, VID sampling width)
2. **Accumulation**: Each `addFrequencyVector` call:
   - Validates vector size consistency
   - Validates non-negative frequency values
   - Aggregates frequencies with coercion to maximum frequency
3. **Computation**: `computeResult` performs:
   - Histogram generation from aggregated frequency vector
   - Differential privacy application via noise addition
   - Reach computation with VID sampling adjustment
   - Frequency distribution computation (for reach-and-frequency only)

**Frequency Vector Format**: ByteArray where each index represents a hashed user ID and the byte value represents observation frequency from a single data provider.
