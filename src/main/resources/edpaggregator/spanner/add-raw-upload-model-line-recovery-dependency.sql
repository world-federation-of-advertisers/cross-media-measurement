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

-- changeset marcopremier:add-raw-upload-model-line-recovery-dependency dbms:cloudspanner
-- comment: Persist operator-eviction recovery dependencies and actions.

SET PROTO_DESCRIPTORS = 'CvEDCl13ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl91cGxvYWRfbW9kZWxfbGluZV9yZWNvdmVyeV9hY3Rpb24ucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKvsBCipSYXdJbXByZXNzaW9uVXBsb2FkTW9kZWxMaW5lUmVjb3ZlcnlBY3Rpb24SQAo8UkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfUkVDT1ZFUllfQUNUSU9OX1VOU1BFQ0lGSUVEEAASQwo/UkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfUkVDT1ZFUllfQUNUSU9OX0VEUF9DT1JSRUNUSU9OEAESRgpCUkFXX0lNUFJFU1NJT05fVVBMT0FEX01PREVMX0xJTkVfUkVDT1ZFUllfQUNUSU9OX09QRVJBVE9SX1JFQ09WRVJZEAJCYgotb3JnLndmYW5ldC5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yQi9SYXdJbXByZXNzaW9uVXBsb2FkTW9kZWxMaW5lUmVjb3ZlcnlBY3Rpb25Qcm90b1ABYgZwcm90bzM=';

START BATCH DDL;

ALTER PROTO BUNDLE INSERT (
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction`
);

ALTER TABLE RawImpressionUploadModelLine ADD COLUMN EvictionOperationId STRING(36);

ALTER TABLE RawImpressionUploadModelLine ADD COLUMN RecoveryAction
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction`;

ALTER TABLE RawImpressionUploadModelLine
  ADD COLUMN RecoveryPredecessorRawImpressionUploadResourceId STRING(63);

RUN BATCH;
