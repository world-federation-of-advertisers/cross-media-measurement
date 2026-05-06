# Security & Cryptography Standards

## What Is This?

This guide covers cryptography and security conventions for WFA repositories,
focusing on correct Tink usage, key management patterns, and envelope
encryption. Tink's API surface is intentional and its encapsulation is part of
the security model. Using internal APIs, bypassing key creation safety, or
mishandling key material are all violations of that model.

## Tink API Usage

([PR #1874](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/1874#issuecomment-2434573770),
[#939](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/939#issuecomment-1507258772))

### Public API Only

Do not use APIs in `com.google.crypto.tink.subtle`. While they may be
generally safe to use, they are not part of Tink's public API and can be
modified or removed at any time. Always use the top-level Tink API.

```kotlin
val aead: Aead = keysetHandle.getPrimitive(Aead::class.java)
```

### Key Creation

Only create keys using Tink's exposed API. Tink intentionally encapsulates
key creation as part of its safety model. Never construct key objects manually,
parse raw key material into Tink key types, or bypass the `KeysetHandle`
abstraction.

```kotlin
val keysetHandle = KeysetHandle.generateNew(
    AesGcmParameters.builder()
        .setKeySizeBytes(32)
        .setIvSizeBytes(12)
        .setTagSizeBytes(16)
        .build()
)
```

Be cautious about key material generated outside of Tink and imported into
Tink structures. This is a potential contraindication to the security model
and should be flagged for review.

### Key Serialization

Keys must be serialized in binary protobuf format using
`TinkProtoKeysetFormat`. Do not use Tink JSON keyset serialization or invent
alternate serialization formats for Tink keysets.
([#1836](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/1836#issuecomment-2391798342))

If an external interface requires text-safe transport, encode the serialized
binary keyset bytes at the boundary (for example, with base64). In that case,
base64 is a transport encoding, not the keyset serialization format.

Exception: some integrations use an encrypted JSON representation of an
application-level wrapper message rather than a serialized Tink keyset. Treat
that as a compatibility format at the system boundary only. It is not a Tink
JSON keyset and should be converted to Tink types immediately after parsing.

### Deprecated APIs

Check Tink release notes for deprecated APIs and use the current alternatives.
For example, `keysetInfo` is deprecated in more recent Tink releases.

## Primitive Registration

### Register Only What You Need

Do not register all Tink configs. Register only the specific primitives your
code uses.
([common-jvm PR #329](https://github.com/world-federation-of-advertisers/common-jvm/pull/329),
[common-jvm PR #320](https://github.com/world-federation-of-advertisers/common-jvm/pull/320)) If your code uses `StreamingAead` but not regular `Aead`, register
only `StreamingAeadConfig`.

Note that envelope encryption is a separate concern — code that performs
envelope encryption is responsible for registering `AeadConfig` for the KEK
independently.

### Registration Location

Tink config registration should happen in the companion object `init` block
of the class that uses the primitive. This guarantees registration happens
before any usage.
([PR #3622](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/3622#issuecomment-4001396341))

```kotlin
class StreamingAeadStorageClient(private val streamingAead: StreamingAead) {
    companion object {
        init {
            StreamingAeadConfig.register()
        }
    }
}
```

## Key Management Patterns

### Primitives at the Edges

Functions should accept Tink primitive objects (`StreamingAead`, `Aead`, etc.),
not raw key material. Keys should be loaded into `KeysetHandle` objects and
wrapped in primitives at system boundaries.
([#781](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/781#issuecomment-1341375697),
[#1947](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/1947#issuecomment-2515037945)) Internal code should never handle
raw key bytes.

### No Magic Strings for Key Types

Do not use string constants to identify key types. The key object already has
its associated type information.

### No Unnecessary Transport Encoding

Do not add base64 or other secondary encodings unless required by an external
system's interface.

## Terminology

([PR #3682](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/3682#issuecomment-4203849190),
[PR #3685](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/3685#issuecomment-4284240764))

*   The cryptographic pattern of wrapping a DEK with a KEK is called "envelope
    encryption" regardless of whether the implementation focuses on encryption
    or decryption. Name classes accordingly (e.g. `EnvelopeEncryptedStorageClient`).
*   DEK (Data Encryption Key) and KEK (Key Encryption Key) are always
    uppercase.
*   In HKDF, the IKM (Input Keying Material) is an input to HMAC, not
    necessarily an encryption key. It is commonly a shared secret such as the
    output of a Diffie-Hellman key exchange.

## See Also

*   [Code Style](code-style.md) — general style rules and language conventions
*   [API & Protobuf Standards](api-standards.md) — API design conventions
    including naming for crypto-related fields
