-- liquibase formatted sql

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

-- changeset marcopremier:add-raw-impression-upload-model-line-availability-sync dbms:cloudspanner
SET PROTO_DESCRIPTORS = 'CsAFClN3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl91cGxvYWRfbW9kZWxfbGluZV9zdGF0ZS5wcm90bxImd2ZhLm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3Iq3QMKIVJhd0ltcHJlc3Npb25VcGxvYWRNb2RlbExpbmVTdGF0ZRI2CjJSQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9VTlNQRUNJRklFRBAAEjIKLlJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0NSRUFURUQQARI5CjVSQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9QT09MX0FTU0lHTklORxACEjIKLlJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX1JBTktJTkcQAxIzCi9SQVdfSU1QUkVTU0lPTl9VUExPQURfTU9ERUxfTElORV9TVEFURV9MQUJFTElORxAEEjQKMFJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0NPTVBMRVRFRBAFEjEKLVJBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0ZBSUxFRBAGEj8KO1JBV19JTVBSRVNTSU9OX1VQTE9BRF9NT0RFTF9MSU5FX1NUQVRFX0FWQUlMQUJJTElUWV9TWU5DSU5HEAdCWQotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQiZSYXdJbXByZXNzaW9uVXBsb2FkTW9kZWxMaW5lU3RhdGVQcm90b1ABYgZwcm90bzM=';

START BATCH DDL;

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

RUN BATCH;
