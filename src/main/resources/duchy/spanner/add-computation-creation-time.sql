-- liquibase formatted sql

-- Copyright 2023 The Cross-Media Measurement Authors
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

-- changeset renjiez:3 dbms:cloudspanner

ALTER TABLE Computations
    -- The time the Computation was created. Due to conflict with default value
    -- and option `allow_commit_timestamp`, this column has to be nullable. The
    -- application code is supposed to check whether the value is null.
    ADD COLUMN CreationTime TIMESTAMP OPTIONS(allow_commit_timestamp = true);
