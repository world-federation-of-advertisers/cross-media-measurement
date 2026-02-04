# org.wfanet.panelmatch.common.certificates.openssl

## Overview
Provides an OpenSSL-based implementation of CertificateAuthority for generating X.509 certificates and private keys by invoking OpenSSL CLI commands in subprocesses. This implementation is intended for testing and development environments only, as it exposes significant security risks including direct root key access and command injection vulnerabilities.

## Components

### OpenSslCertificateAuthority
Certificate authority implementation that generates X.509 certificates by executing OpenSSL commands via subprocess invocation.

**Security Warning:** Not suitable for production use without additional safeguards. Contains command injection vulnerabilities through unsanitized string interpolation and direct access to root private keys.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| generateX509CertificateAndPrivateKey | None | `Pair<X509Certificate, PrivateKey>` | Generates EC key pair and X.509 certificate signed by root CA |

**Constructor Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| context | `CertificateAuthority.Context` | Certificate metadata (CN, organization, DNS, validity) |
| rootPrivateKeyFile | `File` | File path to root CA private key |
| rootCertificateFile | `File` | File path to root CA certificate |
| baseDirectory | `File` | Working directory for temporary certificate generation files |

### GenerateKeyPair (Private)
Internal helper class that orchestrates the multi-step OpenSSL certificate generation process.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| generate | None | `Pair<X509Certificate, PrivateKey>` | Executes complete certificate generation workflow |
| generateCsrAndKey | None | `Unit` | Creates EC private key and certificate signing request |
| writeCnf | None | `Unit` | Writes OpenSSL configuration file with certificate extensions |
| generateX509 | None | `Unit` | Signs CSR with root CA to produce X.509 certificate |

**Generated Certificate Characteristics:**
- Key Algorithm: ECDSA with prime256v1 curve
- Key Usage: nonRepudiation, digitalSignature, keyEncipherment
- Basic Constraints: CA:FALSE (end-entity certificate)
- Subject Alternative Name: Configured from context DNS name
- Authority/Subject Key Identifiers: Automatically generated

## Data Structures

### CertificateAuthority.Context
Configuration object for certificate generation (defined in parent interface).

| Property | Type | Description |
|----------|------|-------------|
| commonName | `String` | Certificate Common Name (CN) field |
| organization | `String` | Certificate Organization (O) field |
| dnsName | `String` | Subject Alternative Name DNS entry |
| validDays | `Int` | Certificate validity period in days |

## Dependencies
- `java.io.File` - File system operations and temporary directory management
- `java.security.PrivateKey` - Private key representation
- `java.security.cert.X509Certificate` - X.509 certificate representation
- `java.util.UUID` - Unique subdirectory generation for isolation
- `org.wfanet.measurement.common.crypto.readCertificate` - PEM certificate parsing
- `org.wfanet.measurement.common.crypto.readPrivateKey` - PEM private key parsing
- `org.wfanet.panelmatch.common.certificates.CertificateAuthority` - Parent interface
- **External:** OpenSSL CLI tool (must be available in system PATH)

## Usage Example
```kotlin
val context = CertificateAuthority.Context(
  commonName = "test-service",
  organization = "ExampleOrg",
  dnsName = "test-service.example.com",
  validDays = 365
)

val ca = OpenSslCertificateAuthority(
  context = context,
  rootPrivateKeyFile = File("/path/to/root-key.pem"),
  rootCertificateFile = File("/path/to/root-cert.pem"),
  baseDirectory = File("/tmp/cert-generation")
)

val (certificate, privateKey) = ca.generateX509CertificateAndPrivateKey()
```

## Implementation Details

### Certificate Generation Workflow
1. Creates UUID-based temporary subdirectory in baseDirectory
2. Generates EC private key (prime256v1) and CSR via `openssl req`
3. Writes OpenSSL configuration file with certificate extensions
4. Signs CSR with root CA using `openssl x509`
5. Reads generated PEM files into Java certificate/key objects
6. Recursively deletes temporary subdirectory

### OpenSSL Commands Executed
```bash
# Step 1: Generate key and CSR
openssl req -out csr -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
  -nodes -keyout key -subj "/O=<org>/CN=<cn>"

# Step 2: Sign CSR with root CA
openssl x509 -in csr -out pem -days <validDays> -req \
  -CA <rootCert> -CAform PEM -CAkey <rootKey> \
  -CAcreateserial -extfile cnf -extensions usr_cert
```

### Security Considerations
- **Command Injection:** Context fields are interpolated into shell commands without sanitization
- **Root Key Exposure:** Direct file system access to root CA private key
- **No Input Validation:** Organization and common name values are not validated
- **Subprocess Errors:** Failures reported via stdout/stderr redirection
- **Temporary Files:** Automatically cleaned up via try-finally block

## Class Diagram
```mermaid
classDiagram
    class CertificateAuthority {
        <<interface>>
        +generateX509CertificateAndPrivateKey() Pair~X509Certificate, PrivateKey~
    }

    class OpenSslCertificateAuthority {
        -context: Context
        -rootPrivateKeyFile: File
        -rootCertificateFile: File
        -baseDirectory: File
        +generateX509CertificateAndPrivateKey() Pair~X509Certificate, PrivateKey~
    }

    class GenerateKeyPair {
        -rootPrivateKeyFile: File
        -rootCertificateFile: File
        -context: Context
        -baseDir: File
        +generate() Pair~X509Certificate, PrivateKey~
        -generateCsrAndKey() Unit
        -writeCnf() Unit
        -generateX509() Unit
    }

    class Context {
        +commonName: String
        +organization: String
        +dnsName: String
        +validDays: Int
    }

    OpenSslCertificateAuthority ..|> CertificateAuthority
    OpenSslCertificateAuthority --> Context
    OpenSslCertificateAuthority ..> GenerateKeyPair : uses
    CertificateAuthority +-- Context
```
