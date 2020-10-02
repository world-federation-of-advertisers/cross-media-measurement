# WFA Measurement System

## Purpose

## Repository Structure

## System Overview

At a high level the system requires at least three independent deployments,
one controller and two secure multiparty computation nodes, each operating its
own microservices and storage instances. In order to make precise statements
about the system architecture we introduce the following terms.

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

1. Advertisers configure reports that may span a variety of campaigns,
   publishers, and forms of media.
1. The Kingdom determines which data are required to generate these reports and
   compiles a list of requisitions for the various publishers. The Kingdom
   tracks which of the requisitions are open and which are fulfilled.
1. Publishers invoke an API on the Duchy to obtain a list of open requisitions.
   The Duchy proxies to the Kingdom to retrieve the list. The requisitions
   specify which data are required from the publisher in order to generate the
   reports.
1. To fulfill the requisitions publishers compute *sketches* similar to those
   used in the HyperLogLog algorithm for cardinality and frequency estimation.
   In practice we do not use HyperLogLog itself due to issues preserving user
   privacy that are beyond the scope of this discussion. Publishers encrypt
   these sketches using the combined public key of all the Duchies. Publishers
   send the encrypted sketches to a Duchy which stores them and informs the
   Kingdom that the requisition for that data is fulfilled. The encrypted
   sketches required for a particular report may be distributed across multiple
   Duchies.
1. The Kingdom determines which pending computations have all necessary
   requisitions fulfilled and are therefore ready to run. The Duchies poll the
   Kingdom at regular intervals to claim this work. Each computation has a
   Primary Duchy assigned and a deterministic order of computation.
1. For each computation all Duchies fetch the required encrypted sketches,
   interleave noise into them, and send them to the Primary Duchy. The Primary
   Duchy writes these sketches to its storage instance as they arrive.
1. Once the Primary Duchy receives all required encrypted noised sketches it
   combines them. Computation then follows the predetermined order making two
   rounds through the Duchies. Each round ends with the Primary Duchy.
1. During the first round each Duchy shuffles the sketches to destroy
   information that could be reconstructed from knowing the register indices.
1. During the second round the Duchies each use their piece of the private key
   to decrypt the results. The Primary Duchy sends the final results back to the
   Kingdom.

## Developer Guide

### Developer Environment

### How to Build

### How to Deploy

## Documentation

* [A System Design for Privacy-Preserving Reach and Frequency Estimation](https://research.google/pubs/pub49526/)
* [Privacy-Preserving Secure Cardinality and Frequency Estimation](https://research.google/pubs/pub49177/)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)