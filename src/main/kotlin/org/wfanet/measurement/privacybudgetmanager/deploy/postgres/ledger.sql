-- Copyright 2025 The Cross-Media Measurement Authors
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


CREATE TYPE ChargesTableState
AS ENUM('NOT_READY', 'BACKFILLING', 'READY');

-- Holds the state and privacyLandscapeName for all the previous (deleted) and current PrivacyCharges tables.
CREATE TABLE PrivacyChargesMetadata (
    -- Name of the Privacy Landscape that is used to map the charges column of this table.
    -- There can be only one table hence, on entry in the metadata table per privacyLandscapeName.
    PrivacyLandscapeName TEXT PRIMARY KEY,
    -- The state of the PrivacyCharges table.
    State ChargesTableState,
    -- Time this PrivacyCharges table was created.
    CreateTime TIMESTAMP NOT NULL,
    -- Time this PrivacyCharges table was deleted, if it was.
    DeleteTime TIMESTAMP
);

-- Holds all the charges. As the privacy landscapes are updated, old PrivacyCharges tables
-- get deleted and migrated to the new PrivacyCharges table which holds in its Charges column, a proto
-- that adheres to the new Privacy Landscape
CREATE TABLE PrivacyCharges(
  -- The EDP these charges belong to.
  EdpId text NOT NULl,
  -- The Measurement Consumer these charges belong to.
  MeasurementConsumerId text NOT NULL,
  -- The Event Group these charges belong to.
  EventGroupId text NOT NULL,
  -- Day for this PrivacyBucket. DD-MM-YYYY.
  Date Date NOT NULL,
  -- A wfa.measurement.privacybudgetmanager.Charges proto capturing charges for each bucket.
  Charges BYTEA,
  -- There can be only one entry per MeasurementConsumerId, EventGroupId, Date triplet.
  PRIMARY KEY (EdpId, MeasurementConsumerId, EventGroupId, Date));

-- Holds all the ledger Entries.
CREATE TABLE LedgerEntries(
  -- The EDP these charges belong to.
  EdpId text NOT NULl,
  -- The Measurement Consumer this Ledger Entry belongs to.
  MeasurementConsumerId text NOT NULL,
  -- ID from an external system that uniquely identifies the source all charges in a transaction
  -- for a given MeasurementConsumer.
  ReferenceId text NOT NULL,
  -- Whether or not the charge is a refund.
  IsRefund Boolean NOT NULL,
  -- Time when the row was inserted.
  CreateTime TIMESTAMP NOT NULL);

-- Used to query references quickly
CREATE
  INDEX LedgerEntriesByReferenceId
ON LedgerEntries(EdpId, MeasurementConsumerId, ReferenceId);