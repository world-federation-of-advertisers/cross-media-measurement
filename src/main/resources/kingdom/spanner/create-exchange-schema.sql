-- liquibase formatted sql

-- Copyright 2020 The Cross-Media Measurement Authors
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

-- changeset efoxepstein:2 dbms:cloudspanner
-- preconditions onFail:MARK_RAN onError:HALT
-- precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'RecurringExchanges'

START BATCH DDL;

-- Cloud Spanner database schema for the Kingdom exchange related tables.
--
-- Table hierarchy:
--   Root
--   ├── ModelProviders
--   │   └── ModelProviderCertificates
--   └── RecurringExchanges
--       └── Exchanges
--           └── ExchangeSteps
--               └── ExchangeStepAttempts
--
-- The important foreign key relationships between the tables are:
--
--   RecurringExchanges <- many:many -> DataProviders (Defined in measurement.sdl)
--   RecurringExchanges <- many:many -> ModelProviders
--
-- Identifiers are random INT64s. APIs (and therefore by extension, UIs) should
-- expose only External identifiers, and ideally only web-safe base64 versions
-- of them without padding (e.g. RFC4648's base64url encoding without padding).
--
-- The schema contains many serialized protocol buffers, usually in two formats:
-- JSON and binary. This may be a little surprising that the data is duplicated.
-- In the long run, we intend to deduplicate this. However, in the short term,
-- JSON provides debugging value.

CREATE TABLE ModelProviders (
  ModelProviderId INT64 NOT NULL,

  ExternalModelProviderId INT64 NOT NULL,
) PRIMARY KEY (ModelProviderId);

CREATE UNIQUE INDEX ModelProvidersByExternalId
  ON ModelProviders(ExternalModelProviderId);

CREATE TABLE ModelProviderCertificates (
  ModelProviderId INT64 NOT NULL,
  CertificateId INT64 NOT NULL,

  ExternalModelProviderCertificateId INT64 NOT NULL,

  FOREIGN KEY (CertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (ModelProviderId, CertificateId),
  INTERLEAVE IN PARENT ModelProviders ON DELETE CASCADE;

CREATE UNIQUE INDEX ModelProviderCertificatesByExternalId
  ON ModelProviderCertificates(ModelProviderId, ExternalModelProviderCertificateId);

-- No Certificate should belong to more than one ModelProvider.
CREATE UNIQUE INDEX ModelProviderCertificatesByCertificateId
  ON ModelProviderCertificates(CertificateId);

CREATE TABLE RecurringExchanges (
  RecurringExchangeId           INT64 NOT NULL,

  ExternalRecurringExchangeId   INT64 NOT NULL,

  ModelProviderId               INT64 NOT NULL,
  DataProviderId                INT64 NOT NULL,

  -- RecurringExchange.State enum as int.
  State                         INT64 NOT NULL,

  NextExchangeDate              DATE NOT NULL,

  -- Serialized RecurringExchangeDetails protocol buffer
  RecurringExchangeDetails      BYTES(MAX) NOT NULL,
  RecurringExchangeDetailsJson  STRING(MAX) NOT NULL,

  FOREIGN KEY (ModelProviderId)
    REFERENCES ModelProviders(ModelProviderId),

  FOREIGN KEY (DataProviderId)
    REFERENCES DataProviders(DataProviderId),
) PRIMARY KEY (RecurringExchangeId);

CREATE UNIQUE INDEX RecurringExchangesByExternalId
  ON RecurringExchanges(ExternalRecurringExchangeId);

CREATE INDEX RecurringExchangesByDataProviderId
  ON RecurringExchanges(DataProviderId);

CREATE INDEX RecurringExchangesByModelProviderId
  ON RecurringExchanges(ModelProviderId);

CREATE INDEX RecurringExchangesByNextExchangeDate
  ON RecurringExchanges(NextExchangeDate);

CREATE TABLE Exchanges (
  RecurringExchangeId  INT64 NOT NULL,
  Date                 DATE NOT NULL,

  -- Exchange.State enum as int.
  State                INT64 NOT NULL,

  -- Serialized ExchangeDetails protocol buffer
  ExchangeDetails      BYTES(MAX) NOT NULL,
  ExchangeDetailsJson  STRING(MAX) NOT NULL,
) PRIMARY KEY (RecurringExchangeId, Date),
  INTERLEAVE IN PARENT RecurringExchanges ON DELETE CASCADE;

CREATE INDEX ExchangesByDate ON Exchanges(Date);

CREATE TABLE ExchangeSteps (
  RecurringExchangeId  INT64 NOT NULL,
  Date                 DATE NOT NULL,
  StepIndex            INT64 NOT NULL,

  -- ExchangeStep.State enum as int.
  State                INT64 NOT NULL,

  UpdateTime           TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),

  -- Denormalized "party" from the parent RecurringExchange.
  -- Exactly one of ModelProviderId and DataProviderId will be set.
  ModelProviderId      INT64,
  DataProviderId       INT64,

  FOREIGN KEY (ModelProviderId)
    REFERENCES ModelProviders(ModelProviderId),

  FOREIGN KEY (DataProviderId)
    REFERENCES DataProviders(DataProviderId),
) PRIMARY KEY (RecurringExchangeId, Date, StepIndex),
  INTERLEAVE IN PARENT Exchanges ON DELETE CASCADE;

CREATE NULL_FILTERED INDEX ExchangeStepsByModelProviderId
  ON ExchangeSteps(ModelProviderId, State);

CREATE NULL_FILTERED INDEX ExchangeStepsByDataProviderId
  ON ExchangeSteps(DataProviderId, State);

CREATE TABLE ExchangeStepAttempts (
  RecurringExchangeId  INT64 NOT NULL,
  Date                 DATE NOT NULL,
  StepIndex            INT64 NOT NULL,
  AttemptIndex         INT64 NOT NULL,

  -- ExchangeStepAttempt.State enum as int.
  State                INT64 NOT NULL,

  -- Time after which the ExchangeStepAttempt has expired and should be marked
  -- as failed.
  ExpirationTime       TIMESTAMP NOT NULL,

  -- Serialized ExchangeStepAttemptDetails protocol buffer
  ExchangeStepAttemptDetails      BYTES(MAX) NOT NULL,
  ExchangeStepAttemptDetailsJson  STRING(MAX) NOT NULL,
) PRIMARY KEY (RecurringExchangeId, Date, StepIndex, AttemptIndex),
  INTERLEAVE IN PARENT ExchangeSteps ON DELETE CASCADE;

RUN BATCH;
