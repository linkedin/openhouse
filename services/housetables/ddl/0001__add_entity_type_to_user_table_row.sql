-- This file is an operations record of a schema change applied out of band by the
-- MySQL/DDS team. The service does not execute it.
--
-- ALGORITHM=INSTANT is requested explicitly. Adding a nullable column with a literal
-- default is eligible for instant DDL from MySQL 8.0.12, subject to table-level
-- restrictions such as row format, row-size and row-version limits, and FULLTEXT
-- indexes. Stating the algorithm makes an ineligible table fail the statement outright
-- rather than silently falling back to a table copy, which on a large table is a very
-- different operation. Instant add-column only supports the last position before
-- MySQL 8.0.29, so no AFTER clause is used: the physical column order therefore
-- differs from schema.sql, which is harmless for named-column access.

ALTER TABLE user_table_row
    ADD COLUMN entity_type VARCHAR (128) DEFAULT NULL,
    ALGORITHM=INSTANT;
