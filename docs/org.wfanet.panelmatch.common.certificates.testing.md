# org.wfanet.panelmatch.common.certificates.testing

## Overview
Provides test implementations of certificate management interfaces for unit testing and development. This package contains mock objects that return fixed test certificates and private keys, eliminating the need for real certificate generation during testing.

## Components

### TestCertificateAuthority
Test implementation of `CertificateAuthority` that returns fixed test credentials.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| generateX509CertificateAndPrivateKey | - | `Pair<X509Certificate, PrivateKey>` | Returns fixed test certificate and private key pair |

**Implementation Notes:**
- Singleton object implementing `CertificateAuthority` interface
- Delegates to `TestCertificateManager` for actual certificate and key values
- Suspending function for compatibility with production implementations

### TestCertificateManager
Test implementation of `CertificateManager` that provides fixed test credentials for all operations.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| getCertificate | `exchange: ExchangeDateKey`, `certName: String` | `X509Certificate` | Returns fixed test certificate regardless of parameters |
| getPartnerRootCertificate | `partnerName: String` | `X509Certificate` | Returns fixed test root certificate |
| getExchangePrivateKey | `exchange: ExchangeDateKey` | `PrivateKey` | Returns fixed test private key |
| getExchangeKeyPair | `exchange: ExchangeDateKey` | `KeyPair` | Returns fixed test certificate, private key, and resource name |
| createForExchange | `exchange: ExchangeDateKey` | `String` | Returns fixed test resource name |

**Constants:**
- `RESOURCE_NAME` - Fixed resource name: `"some-resource-name"`
- `CERTIFICATE` - Lazy-initialized X509 certificate from test data
- `KEY_ALGORITHM` - `"EC"` (Elliptic Curve)

**Implementation Notes:**
- Singleton object implementing `CertificateManager` interface
- All methods return the same fixed test credentials
- Certificate loaded from `TestData.FIXED_SERVER_CERT_PEM_FILE`
- Private key loaded from `TestData.FIXED_SERVER_KEY_FILE` using EC algorithm
- Lazy initialization ensures resources loaded only when needed

## Data Structures

### KeyPair
| Property | Type | Description |
|----------|------|-------------|
| x509Certificate | `X509Certificate` | The X.509 certificate |
| privateKey | `PrivateKey` | The associated private key |
| certName | `String` | Resource name or identifier for the certificate |

## Dependencies
- `org.wfanet.panelmatch.common.certificates` - Provides `CertificateAuthority` and `CertificateManager` interfaces
- `org.wfanet.measurement.common.crypto` - Utilities for reading certificates and private keys
- `org.wfanet.measurement.common.crypto.testing` - Test data including fixed certificate and key files
- `org.wfanet.panelmatch.common` - Provides `ExchangeDateKey` type
- `java.security` - Standard Java security types for certificates and keys

## Usage Example
```kotlin
// Use in tests to avoid real certificate generation
val certAuthority: CertificateAuthority = TestCertificateAuthority
val (certificate, privateKey) = certAuthority.generateX509CertificateAndPrivateKey()

// Use manager for exchange-based operations
val certManager: CertificateManager = TestCertificateManager
val keyPair = certManager.getExchangeKeyPair(exchangeDateKey)
val resourceName = certManager.createForExchange(exchangeDateKey)
```

## Class Diagram
```mermaid
classDiagram
    class CertificateAuthority {
        <<interface>>
        +generateX509CertificateAndPrivateKey() Pair~X509Certificate, PrivateKey~
    }

    class CertificateManager {
        <<interface>>
        +getCertificate(exchange, certName) X509Certificate
        +getPartnerRootCertificate(partnerName) X509Certificate
        +getExchangePrivateKey(exchange) PrivateKey
        +getExchangeKeyPair(exchange) KeyPair
        +createForExchange(exchange) String
    }

    class TestCertificateAuthority {
        <<object>>
        +generateX509CertificateAndPrivateKey() Pair~X509Certificate, PrivateKey~
    }

    class TestCertificateManager {
        <<object>>
        +RESOURCE_NAME String
        +CERTIFICATE X509Certificate
        +PRIVATE_KEY PrivateKey
        +getCertificate(exchange, certName) X509Certificate
        +getPartnerRootCertificate(partnerName) X509Certificate
        +getExchangePrivateKey(exchange) PrivateKey
        +getExchangeKeyPair(exchange) KeyPair
        +createForExchange(exchange) String
    }

    class KeyPair {
        +x509Certificate X509Certificate
        +privateKey PrivateKey
        +certName String
    }

    CertificateAuthority <|.. TestCertificateAuthority
    CertificateManager <|.. TestCertificateManager
    CertificateManager +-- KeyPair
    TestCertificateAuthority --> TestCertificateManager : uses
```
