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

-- changeset tristanvuong2021:14 dbms:cloudspanner
-- validCheckSum: 9:2f1d8b384902dc86e2a5a6e1d91403ac
-- comment: Alter ModelLine Boolean in BasicReport to be NOT NULL with default false

UPDATE BasicReports
SET ModelLineSystemSpecified = false
WHERE ModelLineSystemSpecified IS NULL;

ALTER TABLE BasicReports
  ALTER COLUMN ModelLineSystemSpecified Bool NOT NULL DEFAULT(false);
