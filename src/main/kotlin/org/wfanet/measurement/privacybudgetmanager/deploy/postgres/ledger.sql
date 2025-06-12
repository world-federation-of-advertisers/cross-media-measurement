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


CREATE TYPE ChargesTableState AS ENUM('BACKFILLING', 'READY');


-- Holds the state and privacyLandscapeName for all the previous (deleted) and current PrivacyCharges tables.
-- At any point, there is only one PrivacyCharges table. This table holds charges that adhere to PrivacyLandscape
-- defiened by the PrivacyLandscapeName column in PrivacyChargesMetadata.
-- When a new landscape is added to the system, operator of the PBM runs an offline backfilling operation. 

-- A straightforward way to run this operation is as folloiws: 
-- PrivacyCharges table is renamed to PrivacyChargesOld table. A new PrivacyCharges table with the same 
-- schema is created. 
-- The state of the PrivacyCharges table is marked as BACKFILLING by inserting a new row for the new 
-- landscape into PrivacyChargesMetadata table with the state BACKFILLING. 
-- The offline job reads all the rows from PrivacyChargesOld and maps the Charges column using LandscapeUtils
-- and inserts them into PrivacyCharges table. After all rows are inserted, PrivacyChargesMetadata state 
-- is updated to be READY. Optionally PrivacyChargesOld table is dropped.
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


CREATE TABLE EventDataProviders (
    id SERIAL PRIMARY KEY,
    -- Kingdom resource name for the Edp
    EventDataProviderName TEXT UNIQUE NOT NULL
);

CREATE TABLE MeasurementConsumers (
    id SERIAL PRIMARY KEY,
    -- Kingdom resource name for the MC
    MeasurementConsumerName TEXT UNIQUE NOT NULL
);

CREATE TABLE EventGroupReferences (
    id SERIAL PRIMARY KEY,
    -- Id that is taken from here:
    -- https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/0a67a616d86aa65596bf7eff65d66f74296c7aaf/src/main/proto/wfa/measurement/api/v2alpha/event_group.proto#L64
    EventGroupReferenceId TEXT UNIQUE NOT NULL
);

CREATE TABLE LedgerEntryExternalReferences (
    id SERIAL PRIMARY KEY,
    -- Kingdom resource name for the requisition
    LedgerEntryExternalReferenceName TEXT UNIQUE NOT NULL
);

-- Holds all the charges. As the privacy landscapes are updated, old PrivacyCharges tables
-- get deleted and migrated to the new PrivacyCharges table which holds in its Charges column, a proto
-- that adheres to the new Privacy Landscape.
-- TODO(uakyol) : Consider adding a JSON companion column  for Charges.
CREATE TABLE PrivacyCharges (
    -- Unique row identifier for this charge record.
    id SERIAL PRIMARY KEY,
    -- The integer ID of the EDP these charges belong to.
    EventDataProviderId INTEGER NOT NULL,
    -- The integer ID of the Measurement Consumer these charges belong to.
    MeasurementConsumerId INTEGER NOT NULL,
    -- The integer ID of the Event Group Reference assigned by the EDP that these charges belong to.
    EventGroupReferenceId INTEGER NOT NULL,
    -- Day for this PrivacyBucket. DD-MM-YYYY.
    Date Date NOT NULL,
    -- A wfa.measurement.privacybudgetmanager.Charges proto capturing charges for each bucket.
    Charges BYTEA,
    -- There can be only one entry per EventDataProviderId, MeasurementConsumerId, EventGroupReferenceId triplet on a given Date.
    UNIQUE (EventDataProviderId, MeasurementConsumerId, EventGroupReferenceId, Date),
    FOREIGN KEY (EventDataProviderId) REFERENCES EventDataProviders(id),
    FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers(id),
    FOREIGN KEY (EventGroupReferenceId) REFERENCES EventGroupReferences(id)
);


-- Holds all the ledger Entries with integer IDs and a id
-- There is an argument to hold a many to many mapping table between this table and
-- the PrivacyCharges table. This mapping will not have any use at charge or check time
-- but would serve as another mechanism to validate if the audit logs hold.
CREATE TABLE LedgerEntries (
    -- Unique row identifier for this ledger entry.
    id SERIAL PRIMARY KEY,
    -- The integer ID of the EDP these charges belong to.
    EventDataProviderId INTEGER NOT NULL,
    -- The integer ID of the Measurement Consumer this Ledger Entry belongs to.
    MeasurementConsumerId INTEGER NOT NULL,
    -- The integer ID from an external system that uniquely identifies the source all charges in a transaction
    -- for a given MeasurementConsumer. Likely the requisition id.
    ExternalReferenceId INTEGER NOT NULL,
    -- Whether or not the charge is a refund.
    IsRefund Boolean NOT NULL,
    -- Time when the row was inserted.
    CreateTime TIMESTAMP NOT NULL,
    -- Ensures uniqueness for the combination of EdpId, MeasurementConsumerId, ExternalReferenceId, and CreateTime.
    UNIQUE (EventDataProviderId, MeasurementConsumerId, ExternalReferenceId, CreateTime),
    -- Foreign key constraint to the EdpDimension table.
    FOREIGN KEY (EventDataProviderId) REFERENCES EventDataProviders(id),
    -- Foreign key constraint to the MeasurementConsumers table.
    FOREIGN KEY (MeasurementConsumerId) REFERENCES MeasurementConsumers(id),
    -- Foreign key constraint to the new LedgerEntryExternalReferences table.
    FOREIGN KEY (ExternalReferenceId) REFERENCES LedgerEntryExternalReferences(id)
);