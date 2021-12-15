# Deploying

This guide walks through deploying the Panel Match Client daemon and discusses
which parts can or should be customized in each deployment.

## Overview

[ExchangeWorkflowDaemonMain](https://github.com/world-federation-of-advertisers/panel-exchange-client/blob/main/src/main/kotlin/org/wfanet/panelmatch/client/deploy/ExchangeWorkflowDaemonMain.kt)
is the entryway into the reference implementation.

Throughout, there are comments about what can or should be customized.

## Important Customizations

### Private Keys

A KMS is used to secure per-exchange private keys. The reference implementation
stores the private keys in a StorageClient. To keep them safe, they are
encrypted with AES by the KMS.

#### Basic Configuration

The reference implementation uses Tink to connect to a KMS.

*   Google Cloud Key Management is supported
*   AWS KMS is supported
*   Azure Key Vault is not yet supported (follow
    [this GitHub issue](https://github.com/google/tink/issues/158))
*   HashiCorp Vault is not yet supported (follow
    [this GitHub pull request](https://github.com/google/tink/pull/405))

To set this up in ExchangeWorkflowDaemonMain, set the --tink-key-uri
command-line flag to your AEAD key in your KMS.

#### Advanced Configuration

ExchangeWorkflowDaemonMain only needs the KMS to provide a secure
MutableSecretMap for storing private keys. There are any number of ways the
MutableSecretMap interface could be implemented. For example, rather than using
Tink to connect to the KMS, a KMS client library could be used directly. Or
another mechanism besides KMSes could ensure private keys are protected from
accidental leakage or malicious insiders.

### Certificate Authority

To safeguard root private keys, we strongly recommend using a private
CertificateAuthority (CA).

The CA needs to provide a single operation: given a public key, produce a valid
X509Certificate signed by the owner's root private key.

#### Basic Configuration

The reference implementation *will* include adaptors for:

*   Google Cloud Certificate Authority Service
*   AWS Certificate Manager Private Certificate Authority

We welcome contributions for:

*   HashiCorp Vault PKI Secrets Engine

As of writing, Azure Key Vault does not support this functionality.

#### Advanced Configuration

We recommend generating a new certificate per exchange, but this is not actually
necessary. If there is already a process to generate certificates outside the
reference implementation, that can be used instead.

In this situation, implement a custom
[CertificateAuthority](https://github.com/world-federation-of-advertisers/panel-exchange-client/blob/main/src/main/kotlin/org/wfanet/panelmatch/common/certificates/CertificateAuthority.kt)
subclass that picks the appropriate pre-generated certificate.

### Storage

There are several use cases for private blob storage:

*   Per-exchange artifacts
*   Configuration of which exchanges are valid
*   Storing private keys (see above section on KMSes)
*   Storing partners' root certificates
*   Storing configuration files about which storage systems to use per exchange

#### Basic Configuration

The reference implementation expects the deployer to provide a "Root
StorageClient". Subdirectories within this are used for each of the above-listed
use cases for storage.

In the most basic deployment, simply provide a `rootStorageClient`.

As a best practice, you may also wish to remove all unnecessary entries from
`privateStorageFactories`. This adds defense in depth: a misconfiguration of the
`privateStorageInfo` will then not be able to write sensitive data to the wrong
storage system.

#### Advanced Configuration

`rootStorageClient` can be replaced with different StorageClients per use case.

Some use cases need not use a StorageClient at all as long as the right
interface is implemented. For example, `sharedStorageInfo` could be hard-coded
or passed in as a command-line flag.
