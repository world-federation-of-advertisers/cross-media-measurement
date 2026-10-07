-- Copyright 2026 The Cross-Media Measurement Authors
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

-- liquibase formatted sql

-- changeset marcopremier:add-raw-impression-upload-model-line-availability-sync dbms:cloudspanner
ALTER PROTO BUNDLE UPDATE (
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineState`
);

ALTER TABLE RawImpressionUploadModelLine
  ADD COLUMN PendingAvailabilityDates ARRAY<DATE>;

ALTER TABLE RawImpressionUploadModelLine
  ADD COLUMN MarkAvailabilitySyncingRequestId STRING(36);

CREATE UNIQUE NULL_FILTERED INDEX RawImpressionUploadModelLineByMarkAvailabilitySyncingRequestId
  ON RawImpressionUploadModelLine(
    DataProviderResourceId,
    RawImpressionUploadId,
    MarkAvailabilitySyncingRequestId
  );
