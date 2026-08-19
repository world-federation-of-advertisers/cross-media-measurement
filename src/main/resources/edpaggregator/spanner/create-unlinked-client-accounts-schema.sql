-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
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

-- changeset getina:13 dbms:cloudspanner
-- comment: Create UnlinkedClientAccounts table to persist advertiser client-account reference IDs that EventGroupSync could not resolve to a MeasurementConsumer.

START BATCH DDL;

-- UnlinkedClientAccounts stores the set of advertiser client-account reference
-- IDs that EventGroupSync could not resolve to any MeasurementConsumer
-- ("unlinked"), so they can later be surfaced on a dashboard.
--
-- This is a standalone table (not interleaved). There is one row per unlinked
-- client account, scoped to the DataProvider that observed it. The bare
-- external/resource ID of the DataProvider is stored so that a future per-EDP
-- row-access predicate can match it.
CREATE TABLE UnlinkedClientAccounts (
  -- The globally unique resource ID of the DataProvider that observed this
  -- unlinked client account.
  DataProviderResourceId STRING(63) NOT NULL,
  -- The reference ID of the client account in the DataProvider's ecosystem
  -- that could not be resolved to a MeasurementConsumer.
  ClientAccountReferenceId STRING(36) NOT NULL,
  -- The distinct brands observed for this client account. Display hint only.
  Brands ARRAY<STRING(MAX)>,
  -- One example EventGroup reference ID for this client account, for
  -- traceability. Optional.
  EventGroupReferenceId STRING(MAX),
  -- The time this client account was first observed unlinked in this database.
  FirstObservedTime TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (DataProviderResourceId, ClientAccountReferenceId);

RUN BATCH;
