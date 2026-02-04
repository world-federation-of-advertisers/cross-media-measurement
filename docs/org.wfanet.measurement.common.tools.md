# org.wfanet.measurement.common.tools

## Overview
Provides command-line tooling for OpenID Connect provider operations, including JWKS retrieval and access token generation. This package facilitates integration testing and development workflows requiring OAuth 2.0/OIDC authentication primitives.

## Components

### OpenIdProvider
Command-line interface for OpenID Connect provider operations supporting JWKS export and JWT access token generation.

| Method | Parameters | Returns | Description |
|--------|------------|---------|-------------|
| run | - | `Unit` | Displays command usage information |
| getJwks | - | `Unit` | Prints the JSON Web Key Set to stdout |
| generateAccessToken | `audience: String`, `subject: String`, `scopes: Set<String>?`, `ttl: Duration` | `Unit` | Generates and prints an OAuth 2.0 access token |
| main | `args: Array<String>` | `Unit` | Entry point for command-line execution |

#### Constructor Parameters
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| --issuer | `String` | Yes | OAuth 2.0 issuer identifier |
| --keyset | `File` | Yes | Path to Tink keyset in binary format for token signing |

#### generateAccessToken Parameters
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| --audience | `String` | Yes | - | Target audience claim for the token |
| --subject | `String` | Yes | - | Subject claim identifying the principal |
| --scope | `Set<String>` | No | `null` | OAuth 2.0 scopes (repeatable) |
| --ttl | `Duration` | No | `5m` | Token time-to-live duration |

## Dependencies
- `com.google.crypto.tink` - Cryptographic key management for token signing
- `org.wfanet.measurement.common` - Command-line utilities
- `org.wfanet.measurement.common.grpc.testing` - OpenID provider implementation
- `picocli` - Command-line argument parsing framework

## Usage Example
```kotlin
// Command-line invocation via main method
OpenIdProvider.main(arrayOf(
  "--issuer", "https://example.com",
  "--keyset", "/path/to/keyset.bin",
  "get-jwks"
))

// Generate access token with scopes
OpenIdProvider.main(arrayOf(
  "--issuer", "https://example.com",
  "--keyset", "/path/to/keyset.bin",
  "generate-access-token",
  "--audience", "https://api.example.com",
  "--subject", "user123",
  "--scope", "read",
  "--scope", "write",
  "--ttl", "10m"
))
```

## CLI Commands

### get-jwks
Retrieves and prints the JSON Web Key Set (JWKS) containing public keys for token verification.

**Example:**
```bash
OpenIdProvider --issuer https://example.com --keyset /keys.bin get-jwks
```

### generate-access-token
Creates a signed JWT access token with specified claims and optional scopes.

**Example:**
```bash
OpenIdProvider --issuer https://example.com --keyset /keys.bin \
  generate-access-token \
  --audience https://api.example.com \
  --subject user@example.com \
  --scope read --scope write \
  --ttl 15m
```

## Class Diagram
```mermaid
classDiagram
    class OpenIdProvider {
        -String issuer
        -File keysetFile
        +run() Unit
        +getJwks() Unit
        +generateAccessToken(audience, subject, scopes, ttl) Unit
        +main(args: Array~String~)$ Unit
    }
    OpenIdProvider --> KeysetHandle : uses
    OpenIdProvider --> "org.wfanet.measurement.common.grpc.testing.OpenIdProvider" : delegates to
    OpenIdProvider ..|> Runnable : implements
```
