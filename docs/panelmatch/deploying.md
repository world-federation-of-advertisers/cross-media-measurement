# Deploying

This guide walks through deploying the Panel Exchange Client daemon and
discusses which parts can or should be customized in each deployment.

## Overview

Each Model Provider or Event Data Provider that wishes to use the reference
implementation of the Panel Exchange Client should implement a binary that runs
[ExchangeWorkflowDaemon](../../src/main/kotlin/org/wfanet/panelmatch/client/deploy/ExchangeWorkflowDaemon.kt).

For some examples, see:

*   [ExampleDaemon](../..//src/main/kotlin/org/wfanet/panelmatch/client/deploy/example/ExampleDaemon.kt)
*   [FilesystemExampleDaemonMain](../..//src/main/kotlin/org/wfanet/panelmatch/client/deploy/example/filesystem/FilesystemExampleDaemonMain.kt)
*   [GoogleCloudExampleDaemonMain](../..//src/main/kotlin/org/wfanet/panelmatch/client/deploy/example/gcloud/GoogleCloudExampleDaemonMain.kt)
*   [ExchangeWorkflowDaemonForTest](../..//src/main/kotlin/org/wfanet/panelmatch/integration/ExchangeWorkflowDaemonForTest.kt)

The differences between these examples should illustrate the difference
components that need to be customized.

The section below discusses configuring and customizing ExchangeWorkflowDaemon
and the last section discusses deployment.

### Customizing ExchangeWorkflowDaemon

### Private Keys

A KMS is used to secure per-exchange private keys. The reference implementation
stores the private keys in a StorageClient. To keep them safe, they are
encrypted with AES by the KMS.

The reference implementation uses Tink to connect to a KMS.

*   Google Cloud Key Management is supported
*   AWS KMS is supported
*   Azure Key Vault is not yet supported (follow
    [this GitHub issue](https://github.com/google/tink/issues/158))
*   HashiCorp Vault is not yet supported (follow
    [this GitHub pull request](https://github.com/google/tink/pull/405))

To set this up in ExchangeWorkflowDaemonMain, set the `--tink-key-uri`
command-line flag to your AEAD key in your KMS.

The KMS is only used to provide secure storage for private keys. There are any
number of other ways this could be accomplished. For example, rather than using
Tink to connect to the KMS, a KMS client library could be used directly.
Alternatively, rather than using a KMS, some other mechanism can be employed to
ensure private keys are protected from accidental leakage or malicious insiders
(e.g. storage-layer encryption without human access).

### Certificate Authority

The best practice is to use a CertificateAuthority (CA) to provide X509
Certificates while safeguarding your root private key. The CA needs to provide a
single operation: given a public key, produce a valid X509 Certificate signed by
the owner's root private key.

The reference implementation *will* include adaptors for:

*   Google Cloud Certificate Authority Service
*   AWS Certificate Manager Private Certificate Authority

We welcome contributions for:

*   HashiCorp Vault PKI Secrets Engine

As of writing, Azure Key Vault does not support this functionality.

We recommend generating a new certificate per exchange, but this is not actually
necessary. If there is already a process to generate certificates outside the
reference implementation, that can be used instead.

In this situation, implement a custom
[CertificateAuthority](../..//src/main/kotlin/org/wfanet/panelmatch/common/certificates/CertificateAuthority.kt)
subclass that picks the appropriate pre-generated certificate.

### Storage

There are several use cases for private blob storage:

*   Per-exchange artifacts
*   Configuration of which exchanges are valid
*   Storing private keys (see above section on KMSes)
*   Storing partners' root certificates
*   Storing configuration files about which storage systems to use per exchange

The reference implementation expects the deployer to provide a "Root
StorageClient". Subdirectories within this are used for each of the above-listed
use cases for storage.

The root storage client can be replaced with different StorageClients per use
case.

Some use cases need not use a StorageClient at all as long as the right
interface is implemented. For example, the `SecretMap` holding root certificates
can be backed by some RPC call to an internal, proprietary secret storage
system.

## Deploying

We will provide a reference Kubernetes config alongside the example daemons. To
deploy:

1.  Write a binary that runs `ExchangeWorkflowDaemon`.
2.  Build the binary into a container image.
3.  Customize the Kubernetes configuration. You may need to set some
    command-line flags to configure the daemon.
4.  Deploy on Kubernetes using `kubectl` or your preferred deployment process.
