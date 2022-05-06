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

-- changeset efoxepstein:create-model-providers-table dbms:cloudspanner
CREATE TABLE ModelProviders (
  ModelProviderId INT64 NOT NULL,

  ExternalModelProviderId INT64 NOT NULL,
) PRIMARY KEY (ModelProviderId);

-- changeset efoxepstein:create-model-providers-by-external-id-index dbms:cloudspanner
CREATE UNIQUE INDEX ModelProvidersByExternalId
  ON ModelProviders(ExternalModelProviderId);

-- changeset efoxepstein:create-mp-certs-table dbms:cloudspanner
CREATE TABLE ModelProviderCertificates (
  ModelProviderId INT64 NOT NULL,
  CertificateId INT64 NOT NULL,

  ExternalModelProviderCertificateId INT64 NOT NULL,

  FOREIGN KEY (CertificateId) REFERENCES Certificates(CertificateId),
) PRIMARY KEY (ModelProviderId, CertificateId),
  INTERLEAVE IN PARENT ModelProviders ON DELETE CASCADE;

-- changeset efoxepstein:create-mp-certs-by-external-id-index dbms:cloudspanner
CREATE UNIQUE INDEX ModelProviderCertificatesByExternalId
  ON ModelProviderCertificates(ModelProviderId, ExternalModelProviderCertificateId);

-- No Certificate should belong to more than one ModelProvider.
-- changeset efoxepstein:create-mp-certs-by-cert-id-index dbms:cloudspanner
CREATE UNIQUE INDEX ModelProviderCertificatesByCertificateId
  ON ModelProviderCertificates(CertificateId);

-- changeset efoxepstein:create-recurring-exchanges-table dbms:cloudspanner
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

-- changeset efoxepstein:create-recurring-exchanges-by-external-id-index dbms:cloudspanner
CREATE UNIQUE INDEX RecurringExchangesByExternalId
  ON RecurringExchanges(ExternalRecurringExchangeId);

-- changeset efoxepstein:create-recurring-exchanges-by-edp-id-index dbms:cloudspanner
CREATE INDEX RecurringExchangesByDataProviderId
  ON RecurringExchanges(DataProviderId);

-- changeset efoxepstein:create-recurring-exchanges-by-mp-id-index dbms:cloudspanner
CREATE INDEX RecurringExchangesByModelProviderId
  ON RecurringExchanges(ModelProviderId);

-- changeset efoxepstein:create-recurring-exchanges-by-next-date-index dbms:cloudspanner
CREATE INDEX RecurringExchangesByNextExchangeDate
  ON RecurringExchanges(NextExchangeDate);

-- changeset efoxepstein:create-exchanges-table dbms:cloudspanner
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

-- changeset efoxepstein:create-exchanges-by-date-index dbms:cloudspanner
CREATE INDEX ExchangesByDate ON Exchanges(Date);

-- changeset efoxepstein:create-exchange-steps-table dbms:cloudspanner
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

-- changeset efoxepstein:create-exchange-steps-by-mp-id-index dbms:cloudspanner
CREATE NULL_FILTERED INDEX ExchangeStepsByModelProviderId
  ON ExchangeSteps(ModelProviderId, State);

-- changeset efoxepstein:create-exchange-steps-by-edp-id-index dbms:cloudspanner
CREATE NULL_FILTERED INDEX ExchangeStepsByDataProviderId
  ON ExchangeSteps(DataProviderId, State);

-- changeset efoxepstein:create-exchange-attempts-table dbms:cloudspanner
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
