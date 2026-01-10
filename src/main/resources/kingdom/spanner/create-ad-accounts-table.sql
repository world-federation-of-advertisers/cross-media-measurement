-- liquibase formatted sql

-- Copyright 2025 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset jojijacob:33 dbms:cloudspanner
-- comment: Create AdAccounts lookup table for EventGroupSync MC ID lookup.

START BATCH DDL;

CREATE TABLE AdAccounts (
  MeasurementConsumerId   INT64 NOT NULL,
  DataProviderId          INT64 NOT NULL,
  AdAccountId             INT64 NOT NULL,
  ExternalAdAccountId     INT64 NOT NULL,
  ProvidedAdAccountId     STRING(MAX) NOT NULL,

  FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers(MeasurementConsumerId),
  FOREIGN KEY (DataProviderId) REFERENCES DataProviders(DataProviderId),
) PRIMARY KEY (MeasurementConsumerId, AdAccountId);

-- For API lookups by resource name (global uniqueness of external ID)
CREATE UNIQUE INDEX AdAccountsByExternalId
  ON AdAccounts(ExternalAdAccountId);

-- For ListAdAccounts with DataProvider parent
CREATE INDEX AdAccountsByDataProvider
  ON AdAccounts(DataProviderId);

-- For EventGroupSync lookups by EDP's provided ID
CREATE UNIQUE INDEX AdAccountsByProvidedAdAccountId
  ON AdAccounts(DataProviderId, ProvidedAdAccountId);

RUN BATCH;
