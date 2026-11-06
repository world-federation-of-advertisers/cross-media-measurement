-- liquibase formatted sql

-- Copyright 2026 The Cross-Media Measurement Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- changeset marcopremier:22 dbms:cloudspanner
-- comment: Associate quarantined uploads and persist terminal manifest boundaries.

SET PROTO_DESCRIPTORS = 'CqwECkh3ZmEvbWVhc3VyZW1lbnQvaW50ZXJuYWwvZWRwYWdncmVnYXRvci9yYXdfaW1wcmVzc2lvbl91cGxvYWRfc3RhdGUucHJvdG8SJndmYS5tZWFzdXJlbWVudC5pbnRlcm5hbC5lZHBhZ2dyZWdhdG9yKt0CChhSYXdJbXByZXNzaW9uVXBsb2FkU3RhdGUSKwonUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX1VOU1BFQ0lGSUVEEAASJwojUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX0NSRUFURUQQARImCiJSQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfQUNUSVZFEAISKQolUkFXX0lNUFJFU1NJT05fVVBMT0FEX1NUQVRFX0NPTVBMRVRFRBADEiYKIlJBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9GQUlMRUQQBBIzCi9SQVdfSU1QUkVTU0lPTl9VUExPQURfU1RBVEVfQ09SUkVDVElPTl9SRVFVSVJFRBAFEjsKN1JBV19JTVBSRVNTSU9OX1VQTE9BRF9TVEFURV9SRU1PVkVEX1dJVEhPVVRfUkVQTEFDRU1FTlQQBkJQCi1vcmcud2ZhbmV0Lm1lYXN1cmVtZW50LmludGVybmFsLmVkcGFnZ3JlZ2F0b3JCHVJhd0ltcHJlc3Npb25VcGxvYWRTdGF0ZVByb3RvUAFiBnByb3RvMw==';

START BATCH DDL;

ALTER PROTO BUNDLE UPDATE (
  `wfa.measurement.internal.edpaggregator.RawImpressionUploadState`
);

ALTER TABLE RawImpressionUpload ADD COLUMN CorrectionCandidateId STRING(36);

RUN BATCH;
