# WFA Measurement System

**Table of Contents**

*   [Purpose](#purpose)
*   [System Overview](#system-overview)
*   [Repository Structure](#repository-structure)
    *   [Services and Daemons](#servers-and-daemons)
    *   [Common Directories](#common-directories)
*   [Developer Guide](#developer-guide)
    *   [Developer Environment](#developer-environment)
    *   [How to Build](#how-to-build)
    *   [How to Deploy](#how-to-deploy)
*   [Documentation](#documentation)
    *   [Dependencies](#dependencies)
*   [Contributing](#contributing)

## Purpose

Implementation of a privacy centric system for cross publisher, cross media ads
measurement through secure multiparty computations.

## System Overview

At a high level the system requires at least three independent deployments, one
controller and two secure multiparty computation nodes, each operating its own
microservices and storage instances. In order to make precise statements about
the system architecture we introduce the following terms.

The *Kingdom* is a single deployment that allows advertisers to configure
reports, requests the data and computations required to generate those reports,
and makes the completed reports accessible to advertisers.

The *Duchies* are at least two separate deployments each operated by an
independent organization. The Duchies store the encrypted data and perform the
computations required to generate the reports. Each Duchy holds part of the
private key required to decrypt the data. Therefore all Duchies must participate
in order to perform a computation and decrypt the result. For additional
security the Duchies should be multi-cloud with at least one Duchy deploying to
a different cloud provider than the others.

The system operates as follows. For more detail see the references linked in the
[Documentation](#documentation) section.

1.  Advertisers configure reports that may span a variety of campaigns,
    publishers, and forms of media.
1.  The Kingdom determines which data are required to generate these reports and
    compiles a list of requisitions for the various publishers. The Kingdom
    tracks which of the requisitions are open and which are fulfilled.
1.  Publishers invoke an API on the Duchy to obtain a list of open requisitions.
    The Duchy proxies to the Kingdom to retrieve the list. The requisitions
    specify which data are required from the publisher in order to generate the
    reports.
1.  To fulfill the requisitions publishers compute *sketches* similar to those
    used in the HyperLogLog algorithm for cardinality and frequency estimation.
    In practice we do not use HyperLogLog itself due to issues preserving user
    privacy that are beyond the scope of this discussion. Publishers encrypt
    these sketches using the combined public key of all the Duchies. Publishers
    send the encrypted sketches to a Duchy which stores them and informs the
    Kingdom that the requisition for that data is fulfilled. The encrypted
    sketches required for a particular report may be distributed across multiple
    Duchies.
1.  The Kingdom determines which pending computations have all necessary
    requisitions fulfilled and are therefore ready to run. The Duchies poll the
    Kingdom at regular intervals to claim this work. Each computation has a
    Primary Duchy assigned and a deterministic order of computation.
1.  For each computation all Duchies fetch the required encrypted sketches,
    interleave noise into them, and send them to the Primary Duchy. The Primary
    Duchy writes these sketches to its storage instance as they arrive.
1.  Once the Primary Duchy receives all required encrypted noised sketches it
    combines them. Computation then follows the predetermined order making two
    rounds through the Duchies. Each round ends with the Primary Duchy.
1.  During the first round each Duchy shuffles the sketches to destroy
    information that could be reconstructed from knowing the register indices.
1.  During the second round the Duchies each use their piece of the private key
    to decrypt the results. The Primary Duchy sends the final results back to
    the Kingdom.

## Repository Structure

```
.
├── build  ## Stuff specific to the build system
├── imports  ## Build aliases for external dependencies
│   ├── java
│   └── kotlin
├── src  ## All source code
│   ├── main  ## Source code for production deployments
│   │   ├── cc  ## Crypto library
│   │   ├── kotlin  ## Business Logic, Services, Daemons, DB accessors
│   │   └── proto  ## Service and config definitions
│   └── test  ## Source code for testing code in //src/main/
└── tools
```

Source code packages are grouped by language under the `//src` directory, where
`//src/main` is the code for a production deployment and `//src/test` is code
for unit tests and integration tests of the code under `//src/main`.

The majority of the code is written in Kotlin in the `org.wfanet.measurement`
package. The largest exception is the code to do cryptographic operations, which
is written in C++ with a Java JNI wrapper.

The `//imports` directory contains Bazel build aliases for external
dependencies. No source code, third party or otherwise, is contained in the
imports directory. The alias for a dependency will be in a directory like its
package name. Roughly speaking, the directory structure of the import mirrors
the directory structure of the imported package.

For example, the alias for Java Protobuf
(`@com_google_protobuf//:protobuf_java`) is in
`//imports/java/com/google/protobuf/BUILD.bazel` because the Java package is
`com.google.protobuf`.

The `docker` and `k8s` directories contain schemas and configuration.

### Servers and Daemons

Servers and daemons are both long running jobs deployed to Kubernetes. A key
difference is whether or not they accept RPC communication from other binaries.
In short, a server is a gRPC endpoint, where a daemon is not.

*   Services are defined in proto3 in the `//src/main/proto` directory.
*   Public APIs are defined in `wfa-measurement-proto`, another GitHub
    repository. **TODO: Add link to other GitHub repository.**

```
//src/main/kotlin/org/wfanet/measurement/
├── duchy  ## Duchy specific code
│   ├── daemon  ## Base implementations of daemons run in a Duchy
│   ├── service  ## Base implementations of services run by a Duchy
│   │   └── system  ## Services used within the measurement system
│   └── deploy  ## Deployable artifacts
│       ├── common  ## Servers and daemons that deploy on any cloud
│       ├── gcloud  ## Artifacts specific to Google Cloud deployment
│       └── testing  ## Servers and daemons for testing
└── kingdom  ## Kingdom specific code
    ├── daemon  ## Base implementations of daemons run by the Kingdom
    ├── service  ## Base implementations of services run by the Kingdom
    │   ├── api  ## Public facing services
    │   ├── internal  ## Services used within the Kingdom
    │   └── system  ## Services used within the measurement system
    └── deploy  ## Deployable artifacts
        ├── common  ## Servers and daemons that deploy on any cloud
        └── gcloud  ## Artifacts specific to Google Cloud deployment
```

### Common Directories

Throughout the code there are directories named `common`. These contain code
which is common to multiple packages under the same parent directory. As an
example, `//foo/common` contains code that may be used by other packages under
`//foo`, so `//foo/bar` and `//foo/baz`. The code is not common to packages
under a different parent, i.e. `//foo/common` should not be used by `//bar`.

### Testing Directories Under `//src/main`

In this repository packages under `//src/test` do not depend on other packages
in `//src/test`. As a result test infrastructure code used to test multiple
packages is in a test-only package in `//src/main`. One benefit of such a
structure is that the test infrastructure can also be tested the same way as
production code.

## Developer Guide

### Developer Environment

### How to Build

See [Building](docs/building.md).

### How to Deploy

## Documentation

There is a lot that goes into the design of the system. Too much to cover in
this README alone. More details are covered in:

*   [A System Design for Privacy-Preserving Reach and Frequency Estimation](https://research.google/pubs/pub49526/)
*   [Privacy-Preserving Secure Cardinality and Frequency Estimation](https://research.google/pubs/pub49177/)

### Technologies

*   [Bazel](https://bazel.build/)
*   [Docker](https://www.docker.com/)
*   [gRPC](https://grpc.io/)
*   [Kubernetes (k8s)](https://kubernetes.io/)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
