-- liquibase formatted sql

-- Copyright 2021 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset sanjayvas:1 dbms:cloudspanner
-- preconditions onFail:MARK_RAN onError:HALT
-- precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Measurements'

START BATCH DDL;

-- Cloud Spanner database schema for the Kingdom.
--
-- Table hierarchy:
--   Root
--   ├── Certificates
--   ├── DataProviders
--   │   ├── DataProviderCertificates
--   │   └── EventGroups
--   ├── DuchyCertificates
--   ├── MeasurementConsumerCreationTokens
--   ├── MeasurementConsumers
--   │   ├── MeasurementConsumerApiKeys
--   │   ├── MeasurementConsumerCertificates
--   │   └── Measurements
--   │       ├── ComputationParticipants
--   │       ├── MeasurementLogEntries
--   │       ├── Requisitions
--   │       └── DuchyMeasurementResults
--   └── Accounts
--       ├── OpenIdConnectIdentities
--       ├── OpenIdConnectClaims
--       └── MeasurementConsumersOwners
--
-- Note that there is no Duchies table. Instead, a well-known set of Duchies
-- should be defined in a configuration file for a given Kingdom deployment.
--
-- The important foreign key relationships between the tables are:
--
--   EventGroups -[many:1]-> MeasurementConsumers
--   EventGroups -[many:1]-> DataProviders
--   Requisitions -[many:1]-> Measurements
--
--   MeasurementConsumerCertificates -[many:1]-> MeasurementConsumers
--   DataProviderCertificates -[many:1]-> DataProviders
--   MeasurementConsumerCertificates -[1:1]-> Certificates
--   DataProviderCertificates -[1:1]-> Certificates
--   DuchyCertificates -[1:1]-> Certificates
--
-- Identifiers are random INT64s. APIs (and therefore by extension, UIs) should
-- expose only External identifiers, and ideally only web-safe base64 versions
-- of them without padding (e.g. RFC4648's base64url encoding without padding).
--
-- The schema contains many serialized protocol buffers, usually in two formats:
-- JSON and binary. This may be a little surprising that the data is duplicated.
-- In the long run, we intend to deduplicate this. However, in the short term,
-- JSON provides debugging value.
--
-- Data Providers fetch the unfulfilled Requisitions in their systems, compute
-- the underlying data, and upload them via the public RequisitionFulfillment
-- service.
--
-- Once all Requisitions for a Measurement have been fulfilled, the multi-party
-- computation can begin.

-- X.509 certificates used for consent signaling.
CREATE TABLE Certificates (
  CertificateId INT64 NOT NULL,

  SubjectKeyIdentifier BYTES(MAX) NOT NULL,
  NotValidBefore TIMESTAMP NOT NULL,
  NotValidAfter TIMESTAMP NOT NULL,

  -- org.wfanet.measurement.internal.kingdom.Certificate.RevocationState
  -- protobuf enum encoded as an integer.
  RevocationState INT64 NOT NULL,

  -- Serialized org.wfanet.measurement.internal.kingdom.Certificate.Details
  -- protobuf message.
  CertificateDetails BYTES(MAX),
  CertificateDetailsJson STRING(MAX),
) PRIMARY KEY (CertificateId);

-- Enforce that subject key identifier (SKID) is unique.
CREATE UNIQUE INDEX CertificatesBySubjectKeyIdentifier
  ON Certificates(SubjectKeyIdentifier);

CREATE TABLE DuchyCertificates (
  DuchyId INT64 NOT NULL,
  CertificateId INT64 NOT NULL,

  ExternalDuchyCertificateId INT64 NOT NULL,

  FOREIGN KEY (CertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (DuchyId, CertificateId);

CREATE UNIQUE INDEX DuchyCertificatesByExternalId
  ON DuchyCertificates(DuchyId, ExternalDuchyCertificateId);

CREATE TABLE MeasurementConsumerCreationTokens (
  MeasurementConsumerCreationTokenId INT64 NOT NULL,

  MeasurementConsumerCreationTokenHash BYTES(32) NOT NULL,

  CreateTime        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (MeasurementConsumerCreationTokenId);

CREATE UNIQUE INDEX MeasurementConsumerCreationTokensByHash
  ON MeasurementConsumerCreationTokens(MeasurementConsumerCreationTokenHash);

CREATE TABLE MeasurementConsumers (
  MeasurementConsumerId INT64 NOT NULL,

  PublicKeyCertificateId INT64 NOT NULL,

  ExternalMeasurementConsumerId INT64 NOT NULL,

  MeasurementConsumerDetails BYTES(MAX) NOT NULL,
  MeasurementConsumerDetailsJson STRING(MAX) NOT NULL,

  FOREIGN KEY (PublicKeyCertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (MeasurementConsumerId);

-- For measurement consumer APIs.
CREATE UNIQUE INDEX MeasurementConsumersByExternalId
  ON MeasurementConsumers(ExternalMeasurementConsumerId);

CREATE TABLE MeasurementConsumerApiKeys (
  MeasurementConsumerId INT64 NOT NULL,
  ApiKeyId INT64 NOT NULL,

  ExternalMeasurementConsumerApiKeyId INT64 NOT NULL,

  Nickname STRING(MAX) NOT NULL,
  Description STRING(MAX),

  AuthenticationKeyHash BYTES(32) NOT NULL,
) PRIMARY KEY (MeasurementConsumerId, ApiKeyId),
  INTERLEAVE IN PARENT MeasurementConsumers ON DELETE CASCADE;

CREATE UNIQUE INDEX MeasurementConsumerApiKeysByExternalId
  ON MeasurementConsumerApiKeys(MeasurementConsumerId, ExternalMeasurementConsumerApiKeyId);

CREATE UNIQUE INDEX MeasurementConsumerApiKeysByAuthenticationKeyHash
  ON MeasurementConsumerApiKeys(AuthenticationKeyHash);

CREATE TABLE MeasurementConsumerCertificates (
  MeasurementConsumerId INT64 NOT NULL,
  CertificateId INT64 NOT NULL,

  ExternalMeasurementConsumerCertificateId INT64 NOT NULL,

  FOREIGN KEY (CertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (MeasurementConsumerId, CertificateId),
  INTERLEAVE IN PARENT MeasurementConsumers ON DELETE CASCADE;

CREATE UNIQUE INDEX MeasurementConsumerCertificatesByExternalId
  ON MeasurementConsumerCertificates(MeasurementConsumerId, ExternalMeasurementConsumerCertificateId);

-- No Certificate should belong to more than one MeasurementConsumer.
CREATE UNIQUE INDEX MeasurementConsumerCertificatesByCertificateId
  ON MeasurementConsumerCertificates(CertificateId);

CREATE TABLE DataProviders (
  DataProviderId INT64 NOT NULL,

  PublicKeyCertificateId INT64 NOT NULL,

  ExternalDataProviderId INT64 NOT NULL,

  DataProviderDetails BYTES(MAX) NOT NULL,
  DataProviderDetailsJson STRING(MAX) NOT NULL,

  FOREIGN KEY (PublicKeyCertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (DataProviderId);

-- For data provider APIs.
CREATE UNIQUE INDEX DataProvidersByExternalId
  ON DataProviders(ExternalDataProviderId);

CREATE TABLE DataProviderCertificates (
  DataProviderId INT64 NOT NULL,
  CertificateId INT64 NOT NULL,

  ExternalDataProviderCertificateId INT64 NOT NULL,

  FOREIGN KEY (CertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (DataProviderId, CertificateId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;

CREATE UNIQUE INDEX DataProviderCertificatesByExternalId
  ON DataProviderCertificates(DataProviderId, ExternalDataProviderCertificateId);

-- No Certificate should belong to more than one DataProvider.
CREATE UNIQUE INDEX DataProviderCertificatesByCertificateId
  ON DataProviderCertificates(CertificateId);

-- Each EventGroup belongs to both a MeasurementConsumer and a Data Provider.
--
-- This table is used as follows:
--   * Data Providers inform the Local Measurement Provider of all of their
--     EventGroups and which MeasurementConsumers they belong to. The identifier
--     provided by the Data Provider for the EventGroup is stored as
--     ProvidedEventGroupId.
--   * The system generates the EventGroupId and ExternalEventGroupId.
--   * MeasurementConsumers, when setting up MeasurementSpecs, select a subset
--     of the EventGroups that belong to them.
--   * Each Requisition is a calculation for a specific set of EventGroups over
--     a time window and filtering criteria for each EventGroup.
--
-- This is interleaved under Data Providers to make bulk operations from Data
-- Provider APIs more efficient.
-- TODO: evaluate if interleaving under MeasurementConsumer would be more
-- efficient.
-- TODO(world-federation-of-advertisers/cross-media-measurement#390): Make the
-- following columns NOT NULL: UpdateTime, EventGroupDetails,
-- EventGroupDetailsJson.
CREATE TABLE EventGroups (
  DataProviderId                          INT64 NOT NULL,
  EventGroupId                            INT64 NOT NULL,

  MeasurementConsumerId                   INT64 NOT NULL,

  MeasurementConsumerCertificateId        INT64,

  -- Generated by the system, exposed in UIs.
  ExternalEventGroupId                    INT64 NOT NULL,

  -- Provided by the Data Provider, used for idempotency.
  ProvidedEventGroupId                    STRING(MAX),

  CreateTime        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime        TIMESTAMP OPTIONS (allow_commit_timestamp = true),

  -- wfa.measurement.internal.kingdom.EventGroup.Details serialized proto.
  EventGroupDetails                       BYTES(MAX),
  EventGroupDetailsJson                   STRING(MAX),

  FOREIGN KEY (MeasurementConsumerId)
    REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY (MeasurementConsumerId, MeasurementConsumerCertificateId)
    REFERENCES MeasurementConsumerCertificates(MeasurementConsumerId, CertificateId),
) PRIMARY KEY (DataProviderId, EventGroupId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;

CREATE UNIQUE INDEX EventGroupsByExternalId
  ON EventGroups(DataProviderId, ExternalEventGroupId);
CREATE UNIQUE NULL_FILTERED INDEX EventGroupsByProvidedId
  ON EventGroups(DataProviderId, ProvidedEventGroupId);

CREATE TABLE EventGroupMetadataDescriptors (
  DataProviderId                          INT64 NOT NULL,
  EventGroupMetadataDescriptorId          INT64 NOT NULL,
  ExternalEventGroupMetadataDescriptorId  INT64 NOT NULL,

  -- wfa.measurement.internal.kingdom.EventGroupMetadataDescriptor.Details serialized proto.
  DescriptorDetails                       BYTES(MAX) NOT NULL,
  DescriptorDetailsJson                   STRING(MAX) NOT NULL,
) PRIMARY KEY (DataProviderId, EventGroupMetadataDescriptorId),
  INTERLEAVE IN PARENT DataProviders ON DELETE CASCADE;

CREATE TABLE Measurements (
  MeasurementConsumerId              INT64 NOT NULL,
  MeasurementId                      INT64 NOT NULL,

  ExternalMeasurementId              INT64 NOT NULL,

  -- Globally unique id for the system API so that Duchies can reference a
  -- Measurement via the Computation resource without needing to know the parent
  -- MeasurementConsumerId
  ExternalComputationId              INT64,

  -- Generated by external systems, used for idempotency.
  ProvidedMeasurementId                  STRING(MAX),

  CertificateId                      INT64 NOT NULL,

  CreateTime        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime        TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  -- org.wfanet.measurement.internal.kingdom.Measurement.State Proto enum
  -- encoded as int
  State             INT64 NOT NULL,

  -- org.wfanet.measurement.internal.kingdom.Measurement.Details serialized
  -- proto
  MeasurementDetails     BYTES(MAX) NOT NULL,
  MeasurementDetailsJson STRING(MAX) NOT NULL,

  FOREIGN KEY (MeasurementConsumerId, CertificateId)
    REFERENCES MeasurementConsumerCertificates(MeasurementConsumerId, CertificateId),
) PRIMARY KEY (MeasurementConsumerId, MeasurementId),
  INTERLEAVE IN PARENT MeasurementConsumers ON DELETE CASCADE;

-- Enable finding Measurements ready to be worked on.
CREATE INDEX MeasurementsByState ON Measurements(State, UpdateTime ASC);

-- Enable finding Measurements by externally generated Foreign ids
CREATE UNIQUE NULL_FILTERED INDEX MeasurementsByProvidedId
  ON Measurements(MeasurementConsumerId, ProvidedMeasurementId);

CREATE UNIQUE INDEX MeasurementsByExternalId
  ON Measurements(MeasurementConsumerId, ExternalMeasurementId);

CREATE UNIQUE NULL_FILTERED INDEX MeasurementsByExternalComputationId
  ON Measurements(ExternalComputationId);

-- The Requisition data is actually stored by the Duchy. The Duchy has a map
-- from the ExternalRequisitionId to the blob storage path for the Requisition
-- data (i.e. the bytes provided by the Data Provider).
CREATE TABLE Requisitions (
  MeasurementConsumerId       INT64 NOT NULL,
  MeasurementId               INT64 NOT NULL,
  RequisitionId               INT64 NOT NULL,
  DataProviderId              INT64 NOT NULL,

  UpdateTime                  TIMESTAMP NOT NULL
                              OPTIONS (allow_commit_timestamp = true),

  ExternalRequisitionId       INT64 NOT NULL,

  DataProviderCertificateId   INT64 NOT NULL,

  -- org.wfanet.measurement.internal.kingdom.Requisition.State proto enum
  State                       INT64 NOT NULL,

  -- The ID of the Duchy where the requisition is fulfilled. Otherwise NULL if
  -- the requisition is not yet fulfilled.
  FulfillingDuchyId           INT64,

  -- org.wfanet.measurement.internal.kingdom.RequisitionDetails serialized proto
  RequisitionDetails          BYTES(MAX),
  RequisitionDetailsJson      STRING(MAX),

  FOREIGN KEY (DataProviderId)
    REFERENCES DataProviders(DataProviderId),
  FOREIGN KEY (DataProviderId, DataProviderCertificateId)
    REFERENCES DataProviderCertificates(DataProviderId, CertificateId),
) PRIMARY KEY (MeasurementConsumerId, MeasurementId, RequisitionId),
  INTERLEAVE IN PARENT Measurements ON DELETE CASCADE;

CREATE UNIQUE INDEX RequisitionsByExternalId
  ON Requisitions(DataProviderId, ExternalRequisitionId);

CREATE UNIQUE INDEX RequisitionsByDataProviderId
  ON Requisitions(MeasurementConsumerId, MeasurementId, DataProviderId);

-- Used to effectively list requisitions that a DataProvider would need to
-- fulfill
CREATE INDEX RequisitionsByState ON Requisitions(DataProviderId, State);

-- Stores the details and state of duchies for the computation of parent
-- Measurement.
CREATE TABLE ComputationParticipants (
  MeasurementConsumerId       INT64 NOT NULL,
  MeasurementId               INT64 NOT NULL,
  DuchyId                     INT64 NOT NULL,
  CertificateId               INT64,

  UpdateTime                  TIMESTAMP NOT NULL
                                OPTIONS (allow_commit_timestamp = true),

  State                       INT64 NOT NULL,  -- ParticipantState proto enum

  -- ParticipantDetails serialized proto
  ParticipantDetails          BYTES(MAX) NOT NULL,
  ParticipantDetailsJson      STRING(MAX) NOT NULL,

  FOREIGN KEY (DuchyId, CertificateId)
    REFERENCES DuchyCertificates(DuchyId, CertificateId),
) PRIMARY KEY (MeasurementConsumerId, MeasurementId, DuchyId),
  INTERLEAVE IN PARENT Measurements ON DELETE CASCADE;

-- Contains status updates from the Duchies and within the Kingdom for a
-- particular computation for a Measurement. For any given Measurement, each
-- Duchy might send many updates (one or more per stage of the MPC protocol).
-- This is used to give a bird's eye view of the state of the computations to
-- help debug and track progress.
CREATE TABLE MeasurementLogEntries (
  MeasurementConsumerId INT64 NOT NULL,
  MeasurementId INT64 NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  -- Serialized
  -- org.wfanet.measurement.internal.kingdom.MeasurementLogEntry.Details
  -- protobuf message.
  MeasurementLogDetails      BYTES(MAX) NOT NULL,
  MeasurementLogDetailsJson  STRING(MAX) NOT NULL,
) PRIMARY KEY (MeasurementConsumerId, MeasurementId, CreateTime),
  INTERLEAVE IN PARENT Measurements ON DELETE CASCADE;

-- Duchy-specific information for a Measurement log entry. There should be a row
-- in this table for every row in MeasurementLogEntries where the source of the
-- log event is a Duchy.
CREATE TABLE DuchyMeasurementLogEntries (
  MeasurementConsumerId INT64 NOT NULL,
  MeasurementId INT64 NOT NULL,
  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  DuchyId INT64 NOT NULL,

  ExternalComputationLogEntryId INT64 NOT NULL,

  -- Serialized
  -- org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry.Details
  -- protobuf message.
  DuchyMeasurementLogDetails BYTES(MAX) NOT NULL,
  DuchyMeasurementLogDetailsJson STRING(MAX) NOT NULL,
) PRIMARY KEY (MeasurementConsumerId, MeasurementId, CreateTime),
  INTERLEAVE IN PARENT MeasurementLogEntries ON DELETE CASCADE;

CREATE UNIQUE INDEX DuchyMeasurementLogEntriesByExternalId
  ON DuchyMeasurementLogEntries(DuchyId, ExternalComputationLogEntryId);

CREATE TABLE DuchyMeasurementResults (
  MeasurementConsumerId INT64 NOT NULL,
  MeasurementId INT64 NOT NULL,
  DuchyId INT64 NOT NULL,
  CertificateId INT64 NOT NULL,

  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  EncryptedResult BYTES(MAX) NOT NULL,

  FOREIGN KEY (MeasurementConsumerId, MeasurementId)
      REFERENCES Measurements(MeasurementConsumerId, MeasurementId),
  FOREIGN KEY (DuchyId, CertificateId)
      REFERENCES DuchyCertificates(DuchyId, CertificateId),
) PRIMARY KEY (MeasurementConsumerId, MeasurementId, DuchyId, CertificateId),
  INTERLEAVE IN PARENT Measurements ON DELETE CASCADE;

CREATE TABLE Accounts (
  AccountId INT64 NOT NULL,

  CreatorAccountId INT64,

  ExternalAccountId INT64 NOT NULL,

  -- org.wfanet.measurement.internal.kingdom.Account.ActivationState
  -- protobuf enum encoded as an integer.
  ActivationState INT64 NOT NULL,

  OwnedMeasurementConsumerId INT64,
  ActivationToken INT64 NOT NULL,

  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  UpdateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  FOREIGN KEY (CreatorAccountId) REFERENCES Accounts(AccountId),
  FOREIGN KEY (OwnedMeasurementConsumerId) REFERENCES MeasurementConsumers(MeasurementConsumerId),
) PRIMARY KEY (AccountId);

CREATE UNIQUE INDEX AccountsByExternalId
  ON Accounts(ExternalAccountId);

CREATE TABLE OpenIdConnectIdentities (
  AccountId INT64 NOT NULL,
  OpenIdConnectIdentityId INT64 NOT NULL,

  Issuer STRING(MAX) NOT NULL,
  Subject STRING(MAX) NOT NULL,
) PRIMARY KEY (AccountId, OpenIdConnectIdentityId),
INTERLEAVE IN PARENT Accounts ON DELETE CASCADE;

CREATE UNIQUE INDEX OpenIdConnectIdentitiesByIssuerAndSubject
  ON OpenIdConnectIdentities(Issuer, Subject);

CREATE TABLE OpenIdRequestParams (
  OpenIdRequestParamsId INT64 NOT NULL,

  -- Used as the state
  ExternalOpenIdRequestParamsId INT64 NOT NULL,

  Nonce INT64 NOT NULL,

  CreateTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  ValidSeconds INT64 NOT NULL,
) PRIMARY KEY (OpenIdRequestParamsId);

CREATE UNIQUE INDEX OpenIdRequestParamsByExternalId
  ON OpenIdRequestParams(ExternalOpenIdRequestParamsId);

CREATE TABLE MeasurementConsumerOwners (
  AccountId INT64 NOT NULL,
  MeasurementConsumerId INT64 NOT NULL,

  FOREIGN KEY (AccountId) REFERENCES Accounts(AccountId),
  FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers(MeasurementConsumerId),
) PRIMARY KEY (AccountId, MeasurementConsumerId),
INTERLEAVE IN PARENT Accounts ON DELETE CASCADE;

RUN BATCH;
