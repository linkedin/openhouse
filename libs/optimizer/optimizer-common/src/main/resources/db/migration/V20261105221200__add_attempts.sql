-- Number of attempts in the current retry sequence for a (table_uuid, operation_type): 1 for the
-- first attempt and after each success, incremented per retry. Drives the retry backoff directly on
-- the row, independently of the history windows (which can misalign and miscount a recorded success
-- as a timeout). NOT NULL with a default so existing rows get a valid starting value.
ALTER TABLE table_operations ADD COLUMN attempts INT NOT NULL DEFAULT 1;
