-- liquibase formatted sql

-- changeset ple13:17
ALTER TABLE BasicReports ADD COLUMN AmiMrcExemptedEdps ARRAY<STRING(MAX)>;
