-- liquibase formatted sql

-- Copyright 2024 The Cross-Media Measurement Authors
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

-- changeset sanjayvas:20 dbms:cloudspanner
-- comment: Drop JSON debugging columns.

START BATCH DDL;

ALTER TABLE Certificates
DROP COLUMN CertificateDetailsJson;

ALTER TABLE ComputationParticipants
DROP COLUMN ParticipantDetailsJson;

ALTER TABLE DuchyMeasurementLogEntries
DROP COLUMN DuchyMeasurementLogDetailsJson;

ALTER TABLE EventGroups
DROP COLUMN EventGroupDetailsJson;

ALTER TABLE EventGroupMetadataDescriptors
DROP COLUMN DescriptorDetailsJson;

ALTER TABLE DataProviders
DROP COLUMN DataProviderDetailsJson;

ALTER TABLE ExchangeStepAttempts
DROP COLUMN ExchangeStepAttemptDetailsJson;

ALTER TABLE Exchanges
DROP COLUMN ExchangeDetailsJson;

ALTER TABLE MeasurementConsumers
DROP COLUMN MeasurementConsumerDetailsJson;

ALTER TABLE MeasurementLogEntries
DROP COLUMN MeasurementLogDetailsJson;

ALTER TABLE Measurements
DROP COLUMN MeasurementDetailsJson;

ALTER TABLE RecurringExchanges
DROP COLUMN RecurringExchangeDetailsJson;

ALTER TABLE Requisitions
DROP COLUMN RequisitionDetailsJson;

RUN BATCH;